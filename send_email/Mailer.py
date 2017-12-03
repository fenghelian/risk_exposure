# -*- coding: utf-8 -*-
import ConfigParser
import smtplib
from email.mime.text import MIMEText
# from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import logging
import sys


class Mailer:
    def __init__(self, config_path, account="mail_account"):
        config = ConfigParser.ConfigParser()
        config.read(config_path)
        self.mailto_list = config.get(account, "mailto_list").split(",")
        self.mail_host = config.get(account, "mail_host")
        self.mail_user = config.get(account, "mail_user")
        self.mail_pass = config.get(account, "mail_pass")
        self.mail_postfix = config.get(account, "mail_postfix")

    def send_mail(self, sub, content):  # sub：主题；content：邮件内容
        me = "敞口预测" + "<" + self.mail_user + "@" + self.mail_postfix + ">"

        msg_root = MIMEMultipart('relative')
        msg_root['Subject'] = sub
        msg_root['From'] = me
        msg_root['To'] = ";".join(self.mailto_list)

        # Encapsulate the plain and HTML versions of the message body in an
        # 'alternative' part, so message agents can decide which they want to display.
        msg_alternative = MIMEMultipart('alternative')
        msg_root.attach(msg_alternative)

        msg_text = MIMEText(content, 'html', 'UTF-8')
        msg_alternative.attach(msg_text)

        # This example assumes the image is in the current directory
        # fp = open(image_path, 'rb')
        # msgImage = MIMåEImage(fp.read())
        # fp.close()

        # Define the image's ID as referenced above
        # msgImage.add_header('Content-ID', '<image1>')
        # msgRoot.attach(msgImage)

        # att = MIMEText(open(noclicks_path,'rb').read(),'base64','utf-8')
        # att["Content-Type"] = 'application/octet-stream'
        # att["Content-Disposition"] = 'attachment;filename="no clicks queries.txt"'
        # msgRoot.attach(att)

        # att = MIMEText(open(noresults_path,'rb').read(),'base64','utf-8')
        # att["Content-Type"] = 'application/octet-stream'
        # att["Content-Disposition"] = 'attachment;filename="no results queries.txt"'
        # msgRoot.attach(att)

        try:
            s = smtplib.SMTP()
            s.connect(self.mail_host)  # 连接smtp服务器
            s.login(self.mail_user, self.mail_pass)  # 登陆服务器
            s.sendmail(me, self.mailto_list, msg_root.as_string())  # 发送邮件
            s.close()
            return True
        except Exception, e:
            print str(e)
            return False

if __name__ == '__main__':
    mailer = Mailer('./properties.conf')
    mail_file = sys.argv[1]
    with open(mail_file, 'r') as f:
       lines = f.readlines()
       str = '<br />'.join(lines)

	    
    if mailer.send_mail("催收模型项目报警", str):
        logging.info("report sent successfully.")
    else:
        logging.error("report sent failed.")
