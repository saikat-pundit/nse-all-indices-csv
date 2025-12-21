import imaplib, email, csv, os, sys, re, pytz
from datetime import datetime
from email.header import decode_header

IST = pytz.timezone('Asia/Kolkata')

def decode_text(text):
    if not text: return ""
    return " ".join(
        part.decode(enc if enc else 'utf-8', errors='ignore') if isinstance(part, bytes) else str(part)
        for part, enc in decode_header(text)
    )

def extract_email(from_str):
    match = re.search(r'<([^>]+)>', from_str)
    return match.group(1).split('@')[0] if match else from_str.split('@')[0] if '@' in from_str else from_str

def format_date(date_str):
    try:
        date_obj = email.utils.parsedate_to_datetime(date_str).astimezone(IST)
        return date_obj.strftime('%d %b %H:%M')
    except:
        return ''

def clean_text(text):
    return text.replace(',', '') if text else ''

def fetch_emails():
    user, pwd = os.getenv('YANDEX_EMAIL'), os.getenv('YANDEX_APP_PASSWORD')
    if not user or not pwd: sys.exit('ERROR: Missing credentials')

    try:
        mail = imaplib.IMAP4_SSL('imap.yandex.com', 993)
        mail.login(user, pwd)
        mail.select('INBOX')
        
        _, messages = mail.search(None, 'ALL')
        email_ids = messages[0].split()[-20:]
        
        emails_data = []
        for eid in reversed(email_ids):
            _, msg_data = mail.fetch(eid, '(RFC822)')
            msg = email.message_from_bytes(msg_data[0][1])
            
            date_time = clean_text(format_date(msg.get('Date', '')))
            from_raw = decode_text(msg.get('From', ''))
            from_short = clean_text(extract_email(from_raw))
            subject = clean_text(decode_text(msg.get('Subject', '')))
            
            body = ''
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain' and 'attachment' not in str(part.get('Content-Disposition')):
                        try: body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        except: body = part.get_payload(decode=True).decode('latin-1', errors='ignore')
                        break
            else:
                try: body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
                except: body = msg.get_payload(decode=True).decode('latin-1', errors='ignore')
            
            body_clean = clean_text(body[:200].replace('\n', ' ').strip())
            emails_data.append([date_time, from_short, subject, body_clean])
        
        os.makedirs('Data', exist_ok=True)
        
        old_csv = 'Data/email.csv'
        if os.path.exists(old_csv):
            os.remove(old_csv)
            print(f"🗑️ Deleted old file")
        
        update_time = clean_text(datetime.now(IST).strftime('%d %b %H:%M'))
        emails_data.append(['', '', 'Update Time', update_time])
        
        with open('Data/email.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Date-Time', 'From', 'Subject', 'Body_Preview'])
            writer.writerows(emails_data)
        
        print(f"✅ Saved {len(emails_data)-1} emails + update row (newest first)")
        mail.close()
        mail.logout()
        
    except Exception as e:
        sys.exit(f'ERROR: {e}')

if __name__ == "__main__":
    fetch_emails()
