import datetime
import scipy.stats
import numpy as np
from scipy.stats import norm
from enum import Enum, IntEnum
from scipy.optimize import brentq
from datetime import datetime as dt, timedelta
from numpy import abs as ABS, exp as EXP, log as LOG, sqrt as SQRT
from typing import Tuple, List, Dict, Literal, Union, Any

NORM_CDF = norm.cdf
NORM_PDF = norm.pdf

# Indian Holidays 2025&2026 (Updated)
HOLIDAYS = [
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25", "2026-01-26", "2026-03-03", 
    "2026-03-26", "2026-03-31", "2026-04-03", "2026-04-14", 
    "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14", 
    "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", 
    "2026-12-25", "2026-01-15"
]
CURRENTYEAR = str(dt.now().year)
NEXTYEAR = str(dt.now().year + 1)


class ExpType(Enum):
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


class DayCountType(IntEnum):
    CALENDARDAYS = 365
    BUSINESSDAYS = np.busday_count(
        begindates=str(CURRENTYEAR),
        enddates=str(NEXTYEAR),
        weekmask="1111100",
    )
    TRADINGDAYS = np.busday_count(
        begindates=str(CURRENTYEAR),
        enddates=str(NEXTYEAR),
        weekmask="1111100",
        holidays=HOLIDAYS,
    )


class TryMatchWith(Enum):
    NSE = "NSE"
    CUSTOM = "CUSTOM"
    SENSIBULL = "SENSIBULL"


class FromDateType(IntEnum):
    FIXED = 0
    DYNAMIC = 1


class CalcIvGreeks:
    """Main class for calculating Implied Volatility and Greeks using Black-76 model"""
    
    TD64S = "timedelta64[s]"
    IV_LOWER_BOUND = 1e-11
    SECONDS_IN_A_DAY = np.timedelta64(1, "D").astype(TD64S)

    def __init__(
        self,
        FuturePrice: float,
        AtmStrike: float,
        AtmStrikeCallPrice: float,
        AtmStrikePutPrice: float,
        ExpiryDateTime: dt,
        StrikePrice: Union[float, None] = None,
        StrikeCallPrice: Union[float, None] = None,
        StrikePutPrice: Union[float, None] = None,
        ExpiryDateType: ExpType = ExpType.MONTHLY,
        FromDateTime: Union[dt, None] = None,
        tryMatchWith: TryMatchWith = TryMatchWith.CUSTOM,
        dayCountType: DayCountType = DayCountType.CALENDARDAYS,
        interestRate: float = 0.0,  # Interest rate for discounting only
    ) -> None:
        self.dateFuture = ExpiryDateTime
        self.datePast = dt.now() if FromDateTime is None else FromDateTime
        self.datePastType = (
            FromDateType.FIXED
            if self.datePast.microsecond == FromDateType.FIXED
            else FromDateType.DYNAMIC
        )
        self.dayCountType = dayCountType
        self.tryMatchWith = tryMatchWith
        self.F = FuturePrice  # Futures price is primary input for Black-76
        self.K0 = AtmStrike
        self.C0 = max(AtmStrikeCallPrice, 0.05)  # Minimum price of 5 paisa
        self.P0 = max(AtmStrikePutPrice, 0.05)   # Minimum price of 5 paisa
        self.r = interestRate / 100  # Interest rate only for discounting
        
        # For Black-76: Always use futures price
        self.S = self.F  # Black-76: F is used in place of S*exp(rT)
        
        # Store original prices for reference
        self.original_C0 = AtmStrikeCallPrice
        self.original_P0 = AtmStrikePutPrice
        
        if StrikePrice is not None:
            self.K = StrikePrice
        if StrikeCallPrice is not None:
            self.C = max(StrikeCallPrice, 0.05) if StrikeCallPrice else 0.05
        if StrikePutPrice is not None:
            self.P = max(StrikePutPrice, 0.05) if StrikePutPrice else 0.05
        
        self.T = self.get_tte()
        
        # Validate ATM prices are reasonable
        self._validate_atm_prices()

    def _validate_atm_prices(self):
        """Validate that ATM call and put prices are reasonable"""
        # Check if both ATM prices are zero or very small
        if self.original_C0 <= 0.01 and self.original_P0 <= 0.01:
            print(f"Warning: Both ATM prices are low: Call={self.original_C0}, Put={self.original_P0}")
        
        # Check put-call parity for ATM options
        synthetic_future = self.C0 - self.P0 + self.K0
        price_difference = abs(synthetic_future - self.F)
        
        if price_difference > (self.F * 0.01):  # More than 1% difference
            print(f"Warning: Put-call parity violation for ATM: "
                  f"Future={self.F:.2f}, Synthetic={synthetic_future:.2f}, "
                  f"Diff={price_difference:.2f}")

    def update(
        self,
        FuturePrice: float,
        AtmStrike: float,
        AtmStrikeCallPrice: float,
        AtmStrikePutPrice: float,
        FromDateTime: Union[dt, None] = None,
    ) -> None:
        if FromDateTime is not None:
            self.datePast = FromDateTime
            self.datePastType = FromDateType.FIXED
        
        self.F = FuturePrice
        self.S = self.F  # Update S to current futures price
        self.K0 = AtmStrike
        self.C0 = max(AtmStrikeCallPrice, 0.05)
        self.P0 = max(AtmStrikePutPrice, 0.05)
        self.T = self.get_tte()

    @staticmethod
    def getRiskFreeIntrRate() -> float:
        import pandas as pd
        import requests
        
        try:
            return (
                pd.json_normalize(
                    requests.get(
                        "https://techfanetechnologies.github.io"
                        + "/risk_free_interest_rate/RiskFreeInterestRate.json"
                    ).json()
                )
                .query('GovernmentSecurityName == "364 day T-bills"')
                .reset_index()
                .Percent[0]
            )
        except:
            return 6.0  # Default 6% if fetch fails

    @staticmethod
    def find_atm_strike(all_strikes: List[float], future_price: float) -> float:
        """Find ATM strike based on futures price"""
        return float(min(all_strikes, key=lambda x: abs(x - future_price)))

    def refreshNow(self) -> None:
        if self.datePastType == FromDateType.DYNAMIC:
            self.datePast = dt.now()

    def get_dte(self) -> float:
        if self.dayCountType == DayCountType.CALENDARDAYS:
            return (
                np.datetime64(
                    dt.combine(
                        self.dateFuture.date(), datetime.time(15, 30, 0)
                    )
                )
                - np.datetime64(self.datePast)
            ).astype(self.TD64S) / self.SECONDS_IN_A_DAY
        else:
            return (
                (
                    np.busday_count(
                        begindates=self.datePast.date(),
                        enddates=(self.dateFuture + timedelta(days=1)).date(),
                        weekmask="1111100",
                        holidays=HOLIDAYS,
                    )
                    * self.SECONDS_IN_A_DAY
                )
                - (
                    np.datetime64(
                        int(
                            timedelta(
                                hours=8, minutes=30, seconds=0
                            ).total_seconds()
                        ),
                        "s",
                    )
                ).astype(self.TD64S)
                - (
                    np.datetime64(self.datePast)
                    - np.datetime64(
                        dt.combine(
                            self.datePast.date(), datetime.time(0, 0, 0)
                        )
                    )
                ).astype(self.TD64S)
            ) / self.SECONDS_IN_A_DAY

    def get_tte(self) -> float:
        self.refreshNow()
        return float(
            self.get_dte()
            / (
                self.dayCountType.value
                if (
                    (
                        self.dayCountType == DayCountType.BUSINESSDAYS
                        and self.datePast.year == self.dateFuture.year
                    )
                    or (
                        self.dayCountType == DayCountType.TRADINGDAYS
                        and self.datePast.year == self.dateFuture.year
                    )
                )
                else (
                    np.busday_count(
                        begindates=self.datePast.date(),
                        enddates=f"{self.datePast.year+1}-01-01",
                        weekmask="1111100",
                    )
                    + np.busday_count(
                        begindates=str(self.dateFuture.year),
                        enddates=str(self.dateFuture.year + 1),
                        weekmask="1111100",
                    )
                )
                if (
                    self.dayCountType == DayCountType.BUSINESSDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year == 1)
                )
                else (
                    np.busday_count(
                        begindates=self.datePast.date(),
                        enddates=f"{self.datePast.year+1}-01-01",
                        weekmask="1111100",
                        holidays=HOLIDAYS,
                    )
                    + np.busday_count(
                        begindates=str(self.dateFuture.year),
                        enddates=str(self.dateFuture.year + 1),
                        weekmask="1111100",
                        holidays=HOLIDAYS,
                    )
                )
                if (
                    self.dayCountType == DayCountType.TRADINGDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year == 1)
                )
                else np.busday_count(
                    begindates=self.datePast.date(),
                    enddates=(self.dateFuture + timedelta(days=1)).date(),
                    weekmask="1111100",
                )
                if (
                    self.dayCountType == DayCountType.BUSINESSDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year >= 2)
                )
                else np.busday_count(
                    begindates=self.datePast.date(),
                    enddates=(self.dateFuture + timedelta(days=1)).date(),
                    weekmask="1111100",
                    holidays=HOLIDAYS,
                )
                if (
                    self.dayCountType == DayCountType.TRADINGDAYS
                    and (self.dateFuture.year > self.datePast.year)
                    and (self.dateFuture.year - self.datePast.year >= 2)
                )
                else DayCountType.CALENDARDAYS.value
            )
        )

    def CND(self, d: float):
        A1 = 0.31938153
        A2 = -0.356563782
        A3 = 1.781477937
        A4 = -1.821255978
        A5 = 1.330274429
        RSQRT2PI = 0.39894228040143267793994605993438
        K = 1.0 / (1.0 + 0.2316419 * ABS(d))
        ret_val = (
            RSQRT2PI
            * EXP(-0.5 * d * d)
            * (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5)))))
        )
        return np.where(d > 0, 1.0 - ret_val, ret_val)

    def BSM(self, sigma: float):
        """Black-76 model d1 and d2 calculation"""
        sqrtT = SQRT(self.T)
        # Black-76 formula: d1 = [ln(F/K) + (σ²/2)T] / (σ√T)
        d1 = (LOG(self.F / self.K) + (0.5 * sigma * sigma) * self.T) / (sigma * sqrtT)
        d2 = d1 - sigma * sqrtT
        cndd1, cndd2 = self.CND(d1), self.CND(d2)
        expRT = EXP(-self.r * self.T)
        return expRT, cndd1, cndd2

    def BS_CallPutPrice(self, sigma: float):
        """Black-76 call and put pricing"""
        expRT, cndd1, cndd2 = self.BSM(sigma)
        # Black-76: Call = exp(-rT)[F*N(d1) - K*N(d2)]
        BS_CallPrice = expRT * (self.F * cndd1 - self.K * cndd2)
        # Black-76: Put = exp(-rT)[K*N(-d2) - F*N(-d1)]
        BS_PutPrice = expRT * (self.K * (1.0 - cndd2) - self.F * (1.0 - cndd1))
        return BS_CallPrice, BS_PutPrice

    def BS_CallPrice(self, sigma: float):
        expRT, cndd1, cndd2 = self.BSM(sigma)
        return expRT * (self.F * cndd1 - self.K * cndd2)

    def BS_PutPrice(self, sigma: float):
        expRT, cndd1, cndd2 = self.BSM(sigma)
        return expRT * (self.K * (1.0 - cndd2) - self.F * (1.0 - cndd1))

    def BS_d1(self, sigma: float):
        """Black-76 d1 calculation"""
        if sigma > self.IV_LOWER_BOUND:
            # Black-76: d1 = [ln(F/K) + (σ²/2)T] / (σ√T)
            return (LOG(self.F / self.K) + (sigma**2 / 2) * self.T) / (sigma * SQRT(self.T))
        return np.inf if self.F > self.K else -np.inf

    def BS_d2(self, sigma: float):
        return self.BS_d1(sigma) - (sigma * SQRT(self.T))

    def BS_CallPricing(self, sigma: float):
        """Black-76 call pricing"""
        return EXP(-self.r * self.T) * (NORM_CDF(self.BS_d1(sigma)) * self.F - NORM_CDF(self.BS_d2(sigma)) * self.K)

    def BS_PutPricing(self, sigma: float):
        """Black-76 put pricing"""
        return EXP(-self.r * self.T) * (NORM_CDF(-self.BS_d2(sigma)) * self.K - NORM_CDF(-self.BS_d1(sigma)) * self.F)

    def DeltaCall(self, sigma: float):
        """Black-76 call delta = exp(-rT) * N(d1)"""
        return EXP(-self.r * self.T) * NORM_CDF(self.BS_d1(sigma))

    def DeltaPut(self, sigma: float):
        """Black-76 put delta = exp(-rT) * [N(d1) - 1]"""
        return EXP(-self.r * self.T) * (NORM_CDF(self.BS_d1(sigma)) - 1)

    def Gamma(self, sigma: float) -> float:
        """Black-76 gamma = exp(-rT) * N'(d1) / (F * σ * √T)"""
        if sigma > self.IV_LOWER_BOUND:
            return EXP(-self.r * self.T) * NORM_PDF(self.BS_d1(sigma)) / (self.F * sigma * SQRT(self.T))
        return 0

    def Vega(self, sigma: float) -> float:
        """Black-76 vega = exp(-rT) * F * √T * N'(d1)"""
        return EXP(-self.r * self.T) * NORM_PDF(self.BS_d1(sigma)) * self.F * SQRT(self.T)

    def ThetaCall(self, sigma: float) -> float:
        """Black-76 call theta"""
        return -EXP(-self.r * self.T) * (self.F * sigma * NORM_PDF(self.BS_d1(sigma)) / (2 * SQRT(self.T))) - self.r * self.BS_CallPricing(sigma)

    def ThetaPut(self, sigma: float) -> float:
        """Black-76 put theta"""
        return -EXP(-self.r * self.T) * (self.F * sigma * NORM_PDF(self.BS_d1(sigma)) / (2 * SQRT(self.T))) + self.r * self.BS_PutPricing(sigma)

    def RhoCall(self, sigma: float) -> float:
        """Black-76 call rho = -T * CallPrice"""
        return -self.T * self.BS_CallPricing(sigma)

    def RhoPut(self, sigma: float) -> float:
        """Black-76 put rho = -T * PutPrice"""
        return -self.T * self.BS_PutPricing(sigma)

    def ImplVolWithBrent(self, OptionLtp, PricingFunction):
        try:
            ImplVol = brentq(
                lambda sigma: OptionLtp - PricingFunction(sigma),
                0.001,  # Lower bound
                5.0,    # Upper bound (500% IV)
                xtol=1e-12,
                maxiter=100
            )
            return (
                ImplVol
                if ImplVol > self.IV_LOWER_BOUND
                else self.IV_LOWER_BOUND
            )
        except Exception:
            return self.IV_LOWER_BOUND

    def CallImplVol(self):
        return self.ImplVolWithBrent(self.C, self.BS_CallPricing)

    def PutImplVol(self):
        return self.ImplVolWithBrent(self.P, self.BS_PutPricing)

    def GetImpVolAndGreeks(
        self,
        StrikePrice: Union[float, None] = None,
        StrikeCallPrice: Union[float, None] = None,
        StrikePutPrice: Union[float, None] = None,
        useOtmLiquidity: bool = True,
    ) -> Dict:
        if StrikePrice is not None:
            self.K = StrikePrice
        if StrikeCallPrice is not None:
            self.C = max(StrikeCallPrice, 0.05) if StrikeCallPrice else 0.05
        if StrikePutPrice is not None:
            self.P = max(StrikePutPrice, 0.05) if StrikePutPrice else 0.05
        self.refreshNow()
        
        # Calculate both call and put IV
        CallIV = round(self.CallImplVol(), 6)
        PutIV = round(self.PutImplVol(), 6)
        
        # FIXED: Determine OTM based on futures price, not just ATM strike
        # OTM call when strike >= future price, OTM put when strike < future price
        is_otm_call = self.K >= self.F
        
        # Use OTM option's IV (more liquid and accurate)
        if useOtmLiquidity:
            StrikeIV = CallIV if is_otm_call else PutIV
        else:
            # Use average or other method if needed
            StrikeIV = (CallIV + PutIV) / 2
        
        # Calculate Greeks using unified IV
        Delta = round(self.DeltaCall(StrikeIV), 4)
        
        # Format results
        if self.tryMatchWith == TryMatchWith.NSE:
            _ = {
                "CallIV": round(CallIV * 100, 2),
                "PutIV": round(PutIV * 100, 2),
            }
        else:
            _ = {}
            
        return {
            **{
                "Strike": self.K,
                "FuturePrice": round(self.F, 2),
                "IsOTMCall": is_otm_call,
                "ImplVol": round(StrikeIV * 100, 2),
                "CallIV": round(CallIV * 100, 2),
                "PutIV": round(PutIV * 100, 2),
            },
            **_,
            **{
                "CallDelta": Delta,
                "PutDelta": round(Delta - EXP(-self.r * self.T), 4),
                "Theta": round((self.ThetaPut(StrikeIV) / 365), 4),
                "Vega": round((self.Vega(StrikeIV) / 100), 4),
                "Gamma": round(self.Gamma(StrikeIV), 6),
                "RhoCall": round(self.RhoCall(CallIV) / 100, 4),
                "RhoPut": round(self.RhoPut(PutIV) / 100, 4),
            },
        }
