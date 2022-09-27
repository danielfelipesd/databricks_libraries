import logging
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime

class Slack():
    def __init__(self, processType, process, dbutils):
        self.token = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SlackToken')
        self.channelId = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SlackChannel')
        self.groupId = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SlackGroup')
        self.slackClient = WebClient(token= self.token)
        self.startTime = datetime.now()
        self.startTimeFormatted = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.processType = processType
        self.process = process
    
    def sendRequest(self, data):
        try:
            result = self.slackClient.chat_postMessage(
                    channel=self.channelId,
                    attachments = data
            )
            return result
        except Exception as e:
            raise e
    
    def sendThreadMessage(self, threadTimestamp):
        try:
            self.slackClient.chat_postMessage(
                    channel = self.channelId,
                    thread_ts = threadTimestamp,
                    text=f"<!subteam^{self.groupId}> :eyesshaking: *Failed!*"
                )
        except Exception as e:
            raise e
    
    def buildStartMessage(self):
        try:
            message =  [
                {
                    "color": "#7A918F",
                    "fallback": f"Message not rendering, please <!subteam^{self.groupId}> take a look",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f":databricks: Process Notification - {self.processType}",
                                "emoji": True
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f":loading: *Running:* {self.process}"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f":stopwatch: *Started at* {self.startTimeFormatted}"
                            }
                        }
                    ]
                }
            ]
            return message
        except Exception as e:
            print(e)
        
    def buildEndMessage(self, status):
        try:
            endTimeFormatted = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            timeInSeconds = str(datetime.now() - self.startTime).split(".")[0]
            if status.lower() == 'success':
                emoji = ':white_check_mark:'
                collor = '#0DCA07'
            elif status.lower() == 'failed':
                emoji = ':X:'
                collor = '#F20808'
            else:
                emoji = ':question:'
                collor = '#7A918F'
            message = [
                {
                    "color": f"{collor}",
                    "fallback": f"Message not rendering, please <!subteam^{self.groupId}> take a look",
                    "blocks": [
                                {
                                    "type": "header",
                                    "text": {
                                        "type": "plain_text",
                                        "text": f":databricks: Process Notification - {self.processType}",
                                        "emoji": True
                                    }
                                },
                                {
                                    "type": "section",
                                    "text": {
                                        "type": "mrkdwn",
                                        "text": f":gear:  *Process:* {self.process}"
                                    }
                                },
                                {
                                    "type": "section",
                                    "text": {
                                        "type": "mrkdwn",
                                        "text": f"{emoji}  *Status:* *{status.upper()}*"
                                    }
                                },
                                {
                                    "type": "section",
                                    "text": {
                                        "type": "mrkdwn",
                                        "text": f":stopwatch:  *Started at:* {self.startTimeFormatted}"
                                    }
                                },
                                {
                                    "type": "section",
                                    "text": {
                                        "type": "mrkdwn",
                                        "text": f":stopwatch:  *Ended at:* {endTimeFormatted}"
                                    }
                                },
                                {
                                    "type": "section",
                                    "text": {
                                        "type": "mrkdwn",
                                        "text": f":stopwatch:  *Runned for:* {timeInSeconds} seconds"
                                    }
                                }
                            ]
                }
            ]
            return message
        except Exception as e:
            print(e)
    
    def sendInitialMessage(self):
        try:
            message = self.buildStartMessage()
            self.sendRequest(message)
            logging.info("message send successful")
        except Exception as e:
            raise e
    
    def sendFinalMessage(self, status):
        try:
            message = self.buildEndMessage(status)
            resp = self.sendRequest(message)
            if status.lower() != 'success':
                threadTimestamp = resp.get("message").get('ts')
                self.sendThreadMessage(threadTimestamp)
            logging.info("message send successful")
        except Exception as e:
            raise e