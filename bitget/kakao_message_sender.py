# -*- coding: utf-8 -*-
"""
카카오톡 메시지 발송 모듈
다른 프로젝트에서 import하여 사용 가능

사용 예시:
    from kakao_sender import KakaoMessageSender
    
    sender = KakaoMessageSender("REST_API_KEY", "REDIRECT_URI")
    sender.send_message("안녕하세요!")
"""

import requests
import json
import os
from datetime import datetime, timedelta


class KakaoMessageSender:
    """카카오톡 메시지 전송 클래스"""
    
    def __init__(self, rest_api_key, redirect_uri, client_secret=None, token_file="kakao_token.json"):
        """
        초기화
        
        Args:
            rest_api_key (str): 카카오 REST API 키
            redirect_uri (str): 리다이렉트 URI
            client_secret (str, optional): Client Secret (필요한 경우)
            token_file (str, optional): 토큰 저장 파일명. 기본값: "kakao_token.json"
        """
        self.rest_api_key = rest_api_key
        self.redirect_uri = redirect_uri
        self.client_secret = client_secret
        self.token_file = token_file
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        
        # 자동으로 저장된 토큰 로드
        self.load_token()
    
    def save_token(self):
        """토큰을 파일에 저장"""
        token_data = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_expiry": self.token_expiry.isoformat() if self.token_expiry else None,
            "saved_at": datetime.now().isoformat()
        }
        
        with open(self.token_file, 'w', encoding='utf-8') as f:
            json.dump(token_data, f, indent=2, ensure_ascii=False)
    
    def load_token(self):
        """저장된 토큰을 파일에서 불러오기"""
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r', encoding='utf-8') as f:
                    token_data = json.load(f)
                
                self.access_token = token_data.get("access_token")
                self.refresh_token = token_data.get("refresh_token")
                
                expiry_str = token_data.get("token_expiry")
                if expiry_str:
                    self.token_expiry = datetime.fromisoformat(expiry_str)
                
                print(f'success load token_file: {self.token_file}')
                return True
            except Exception as e:
                print(f'failed load token_file: {self.token_file}')
                return False
        return False
    
    def get_authorization_url(self):
        """
        인가 코드 받기 위한 URL 생성
        
        Returns:
            str: 카카오 로그인 URL
        """
        auth_url = (
            f"https://kauth.kakao.com/oauth/authorize"
            f"?client_id={self.rest_api_key}"
            f"&redirect_uri={self.redirect_uri}"
            f"&response_type=code"
            f"&scope=talk_message"
        )
        return auth_url
    
    def get_tokens(self, authorization_code):
        """
        인가 코드로 액세스 토큰 발급
        
        Args:
            authorization_code (str): 카카오 로그인 후 받은 인가 코드
            
        Returns:
            dict: 토큰 정보 또는 None
        """
        token_url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": self.rest_api_key,
            "redirect_uri": self.redirect_uri,
            "code": authorization_code
        }
        
        if self.client_secret:
            data["client_secret"] = self.client_secret
        
        response = requests.post(token_url, data=data)
        tokens = response.json()
        
        if "access_token" in tokens:
            self.access_token = tokens["access_token"]
            self.refresh_token = tokens.get("refresh_token")
            
            expires_in = tokens.get("expires_in", 21600)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            self.save_token()
            return tokens
        else:
            return None
    
    def refresh_access_token(self):
        """
        리프레시 토큰으로 액세스 토큰 갱신
        
        Returns:
            dict: 갱신된 토큰 정보 또는 None
        """
        if not self.refresh_token:
            return None
        
        token_url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.rest_api_key,
            "refresh_token": self.refresh_token
        }
        
        if self.client_secret:
            data["client_secret"] = self.client_secret
        
        response = requests.post(token_url, data=data)
        tokens = response.json()
        
        if "access_token" in tokens:
            self.access_token = tokens["access_token"]
            
            if "refresh_token" in tokens:
                self.refresh_token = tokens["refresh_token"]
            
            expires_in = tokens.get("expires_in", 21600)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            self.save_token()
            return tokens
        else:
            return None
    
    def ensure_valid_token(self):
        """
        토큰 유효성 확인 및 자동 갱신
        
        Returns:
            bool: 유효한 토큰 존재 여부
        """
        if not self.access_token:
            return False
        
        # 토큰 만료 10분 전에 자동 갱신
        if self.token_expiry and datetime.now() >= self.token_expiry - timedelta(minutes=10):
            return self.refresh_access_token() is not None
        
        return True
    
    def send_message(self, text):
        """
        텍스트 메시지 전송 (간편 메서드)
        
        Args:
            text (str): 전송할 메시지
            
        Returns:
            bool: 전송 성공 여부
        """
        return self.send_text_message(text)
    
    def send_text_message(self, text):
        """
        텍스트 메시지 전송
        
        Args:
            text (str): 전송할 메시지
            
        Returns:
            bool: 전송 성공 여부
        """
        if not self.ensure_valid_token():
            raise Exception("유효한 토큰이 없습니다. get_tokens()로 토큰을 먼저 발급받으세요.")
        
        url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        template = {
            "object_type": "text",
            "text": text,
            "link": {
                "web_url": "https://developers.kakao.com",
                "mobile_web_url": "https://developers.kakao.com"
            }
        }
        
        data = {
            "template_object": json.dumps(template)
        }
        
        response = requests.post(url, headers=headers, data=data)
        
        if response.status_code == 200:
            return True
        else:
            result = response.json()
            raise Exception(f"메시지 전송 실패: {result}")
    
    def send_feed_message(self, title, description, image_url, link_url):
        """
        피드 형식 메시지 전송
        
        Args:
            title (str): 제목
            description (str): 설명
            image_url (str): 이미지 URL
            link_url (str): 링크 URL
            
        Returns:
            bool: 전송 성공 여부
        """
        if not self.ensure_valid_token():
            raise Exception("유효한 토큰이 없습니다. get_tokens()로 토큰을 먼저 발급받으세요.")
        
        url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        template = {
            "object_type": "feed",
            "content": {
                "title": title,
                "description": description,
                "image_url": image_url,
                "link": {
                    "web_url": link_url,
                    "mobile_web_url": link_url
                }
            }
        }
        
        data = {
            "template_object": json.dumps(template)
        }
        
        response = requests.post(url, headers=headers, data=data)
        
        if response.status_code == 200:
            return True
        else:
            result = response.json()
            raise Exception(f"메시지 전송 실패: {result}")
    
    def is_token_valid(self):
        """
        현재 토큰이 유효한지 확인
        
        Returns:
            bool: 토큰 유효 여부
        """
        if not self.access_token:
            return False
        
        if self.token_expiry and datetime.now() >= self.token_expiry:
            return False
        
        return True


# 테스트 및 최초 설정용 함수
def setup_kakao_sender():
    """최초 1회 설정 (토큰 발급)"""
    print("="*60)
    print("카카오톡 메시지 발송 초기 설정")
    print("="*60)
    
    REST_API_KEY = "fdb46f8c2665f76124d491f96736c1c1"  # REST API 키
    REDIRECT_URI = "http://localhost:8080"  # Redirect URI
    CLIENT_SECRET = 'gKcSNxUDhYgjsF3eLo5xVJoNFA62uqKB' 
    CLIENT_SECRET = CLIENT_SECRET if CLIENT_SECRET else None
    
    sender = KakaoMessageSender(REST_API_KEY, REDIRECT_URI, CLIENT_SECRET)
    
    if not sender.access_token:
        print("\n인가 코드를 받기 위해 아래 URL을 브라우저에서 열어주세요:")
        print(sender.get_authorization_url())
        
        code = input("\n인가 코드를 입력하세요: ").strip()
        
        if sender.get_tokens(code):
            print("\n✅ 설정 완료!")
            print("이제 다른 파일에서 import하여 사용할 수 있습니다.")
        else:
            print("\n❌ 토큰 발급 실패")
    else:
        print("\n✅ 이미 토큰이 존재합니다.")

    return sender
# 이 파일을 직접 실행했을 때만 실행
if __name__ == "__main__":
    sender = setup_kakao_sender()
    sender.send_text_message('전송준비완료')