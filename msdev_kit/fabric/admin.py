import requests
import json
from typing import Dict

class Admin:

    def __init__(self, token: str):
        """
        Initialize variables for Power BI Admin API interactions.

        Args:
            token (str): The bearer token for authorization.
        """
        self.main_url = 'https://api.powerbi.com/v1.0/myorg/admin'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        

    def get_report_users_as_admin(self, report_id: str) -> Dict:
        """
        Retrieves a list of users with access to a specific report as an administrator.

        Args:
            report_id (str): The ID of the report to get users for.

        Returns:
            Dict: A dictionary containing the status message and content (list of users).
        """
        request_url = f'{self.main_url}/reports/{report_id}/users'
        
        r = requests.get(url=request_url, headers=self.headers)

        status = r.status_code
        response = json.loads(r.content)

        if status == 200:
            return {'message': 'Success', 'content': response.get('value', [])}
        else:
            error_message = response.get('error', {}).get('message', 'Unknown error')
            return {'message': {'error': error_message, 'status_code': status}, 'content': ''}


