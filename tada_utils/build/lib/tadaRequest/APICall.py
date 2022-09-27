import requests

class APICall:

    def request(self,url,requestType,header,body=None):
        if requestType == 'get':
            response = requests.get(url, headers=header)
        elif requestType == 'post':
            response = requests.post(url, headers=header, json=body)
        else:
            print('Request type not supported')

        return response