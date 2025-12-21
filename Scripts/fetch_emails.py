import imaplib, email, csv, os, sys, pytz
from datetime import datetime
from email.header import decode_header

IST = pytz.timezone('Asia/Kolkata')

def decode_text(text):
    if not text: return ""
    return " ".join(
        part.decode(enc if enc else 'utf-8', errors='ignore') if isinstance(part, bytes) else str(part)
        for part, enc in decode_header(text)
    )

def fetch_emails():
    user, pwd = os.getenv('YANDEX_EMAIL'), os.getenv('YANDEX_APP_PASSWORD')
    if not user or not pwd: sys.exit('ERROR: Missing credentials')

    try:
        mail = imaplib.IMAP4_SSL('imap.yandex.com', 993)
        mail.login(user, pwd)
        mail.select('INBOX')
        
        _, messages = mail.search(None, 'ALL')
        email_ids = messages[0].split()[-100:]  # Last 100 emails
        
        emails_data = []
        for eid in email_ids:
            _, msg_data = mail.fetch(eid, '(RFC822)')
            msg = email.message_from_bytes(msg_data[0][1])
            
            date_str = msg.get('Date', '')
            try:
                date_obj = email.utils.parsedate_to_datetime(date_str).astimezone(IST)
                date_time = date_obj.strftime('%Y-%m-%d %H:%M:%S')
            except:
                date_time = ''
            
            from_ = decode_text(msg.get('From', ''))
            subject = decode_text(msg.get('Subject', ''))
            
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
            
            emails_data.append([date_time, from_, subject, body[:200].replace('\n', ' ').strip()])
        
        os.makedirs('Data', exist_ok=True)
        with open('Data/email.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Date-Time', 'From', 'Subject', 'Body_Preview'])
            writer.writerows(emails_data)
        
        print(f"✅ Saved {len(emails_data)} emails")
        mail.close()
        mail.logout()
        
    except Exception as e:
        sys.exit(f'ERROR: {e}')

if __name__ == "__main__":
    fetch_emails()
