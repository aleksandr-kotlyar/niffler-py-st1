import base64
import hashlib
import logging
import os
import re
from urllib.parse import urlparse, parse_qs

import curlify
from requests import Session

from models.config import Envs


class AuthSession(Session):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.code = None

    def request(self, method, url, **kwargs):
        response = super().request(method, url, **kwargs)
        logging.info(curlify.to_curl(response.request))
        logging.info(response.text)
        logging.info(response.headers)
        for r in response.history:
            cookies = r.cookies.get_dict()
            self.cookies.update(cookies)
            code = parse_qs(urlparse(r.headers.get("Location")).query).get("code", None)
            if code:
                self.code = code
        return response


class AuthClient:
    def __init__(self, env: Envs):
        self.session = AuthSession()
        self.domain_url = env.auth_url
        self.code_verifier = base64.urlsafe_b64encode(os.urandom(32)).decode('utf-8')
        self.code_verifier = re.sub('[^a-zA-Z0-9]+', '', self.code_verifier)

        self.code_challenge = hashlib.sha256(self.code_verifier.encode('utf-8')).digest()
        self.code_challenge = base64.urlsafe_b64encode(self.code_challenge).decode('utf-8')
        self.code_challenge = self.code_challenge.replace('=', '')

        self._basic_token = base64.b64encode(env.auth_secret.encode('utf-8')).decode('utf-8')
        self.authorization_basic = {"Authorization": f"Basic {self._basic_token}"}
        self.token = None

    def auth(self, username, password):
        self.session.get(
            url=f"{self.domain_url}/oauth2/authorize",
            params={
                "response_type": "code",
                "client_id": "client",
                "scope": "openid",
                "redirect_uri": "http://frontend.niffler.dc/authorized",
                "code_challenge": self.code_challenge,
                "code_challenge_method": "S256",
            },
            allow_redirects=True
        )

        self.session.post(
            url=f"{self.domain_url}/login",
            data={
                "username": username,
                "password": password,
                "_csrf": self.session.cookies.get("XSRF-TOKEN")
            },
            allow_redirects=True
        )

        token_response = self.session.post(
            url=f"{self.domain_url}/oauth2/token",
            data={
                "code": self.session.code,
                "redirect_uri": "http://frontend.niffler.dc/authorized",
                "code_verifier": self.code_verifier,
                "grant_type": "authorization_code",
                "client_id": "client"
            },
            headers=self.authorization_basic,
        )

        self.token = token_response.json().get("access_token", None)
        return self.token

    def register(self, username, password):
        self.session.get(
            url=f"{self.domain_url}/register",
            params={
                "redirect_uri": "http://auth.niffler.dc:9000/register",
            },
            allow_redirects=True
        )

        result = self.session.post(
            url=f"{self.domain_url}/register",
            data={
                "username": username,
                "password": password,
                "passwordSubmit": password,
                "_csrf": self.session.cookies.get("XSRF-TOKEN")
            },
            allow_redirects=True
        )
        return result
