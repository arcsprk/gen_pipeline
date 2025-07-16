import yaml
import requests
import json
from typing import Dict, Any, Optional


def process_yaml_with_api(
    input_yaml_path: str,
    output_yaml_path: str,
    input_keys: list,
    output_keys: list,
    api_url: str,
    api_method: str = "POST",
    api_headers: Optional[Dict[str, str]] = None,
    api_params: Optional[Dict[str, Any]] = None,
    request_body_template: Optional[Dict[str, Any]] = None
) -> bool:
    """
    YAML 파일에서 특정 키 경로의 값을 읽어 API 호출 후 결과를 새로운 YAML 파일에 저장
    
    Args:
        input_yaml_path: 입력 YAML 파일 경로
        output_yaml_path: 출력 YAML 파일 경로
        input_keys: 입력값을 찾을 키 경로 리스트 (예: ['key1', 'key2'])
        output_keys: 출력값을 저장할 키 경로 리스트 (예: ['key1', 'key2', 'key3'])
        api_url: API 엔드포인트 URL
        api_method: HTTP 메서드 (기본값: POST)
        api_headers: API 헤더 (선택사항)
        api_params: API 파라미터 (선택사항)
        request_body_template: 요청 본문 템플릿 (선택사항)
    
    Returns:
        bool: 성공 여부
    """
    
    try:
        # 1. 입력 YAML 파일 읽기
        with open(input_yaml_path, 'r', encoding='utf-8') as file:
            input_data = yaml.safe_load(file)
        
        # 2. 특정 키 경로에서 input_text 추출
        input_text = get_nested_value(input_data, input_keys)
        if input_text is None:
            print(f"입력 키 경로 {' -> '.join(input_keys)}에서 값을 찾을 수 없습니다.")
            return False
        
        print(f"추출된 input_text: {input_text}")
        
        # 3. API 호출 준비
        headers = api_headers or {'Content-Type': 'application/json'}
        
        # 요청 본문 구성
        if request_body_template:
            # 템플릿에서 {input_text} 플레이스홀더를 실제 값으로 대체
            request_body = json.loads(
                json.dumps(request_body_template).replace('{input_text}', str(input_text))
            )
        else:
            # 기본 요청 본문
            request_body = {"text": input_text}
        
        # 4. API 호출
        print(f"API 호출: {api_method} {api_url}")
        
        if api_method.upper() == "GET":
            params = api_params or {}
            params.update(request_body)
            response = requests.get(api_url, headers=headers, params=params)
        else:
            response = requests.request(
                method=api_method.upper(),
                url=api_url,
                headers=headers,
                json=request_body,
                params=api_params
            )
        
        # 응답 확인
        response.raise_for_status()
        api_result = response.json()
        
        print(f"API 응답 상태: {response.status_code}")
        print(f"API 응답: {api_result}")
        
        # 5. 출력 YAML 파일 생성 및 저장
        output_data = create_nested_structure(output_keys, api_result)
        
        with open(output_yaml_path, 'w', encoding='utf-8') as file:
            yaml.dump(output_data, file, default_flow_style=False, allow_unicode=True)
        
        print(f"결과가 {output_yaml_path}에 저장되었습니다.")
        return True
        
    except FileNotFoundError:
        print(f"파일을 찾을 수 없습니다: {input_yaml_path}")
        return False
    except requests.RequestException as e:
        print(f"API 호출 오류: {e}")
        return False
    except yaml.YAMLError as e:
        print(f"YAML 파싱 오류: {e}")
        return False
    except Exception as e:
        print(f"예상치 못한 오류: {e}")
        return False


def get_nested_value(data: Dict[str, Any], keys: list) -> Any:
    """
    중첩된 딕셔너리에서 키 경로를 따라 값을 추출
    """
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def create_nested_structure(keys: list, value: Any) -> Dict[str, Any]:
    """
    키 경로를 따라 중첩된 딕셔너리 구조를 생성
    """
    if not keys:
        return value
    
    result = {}
    current = result
    
    for i, key in enumerate(keys[:-1]):
        current[key] = {}
        current = current[key]
    
    current[keys[-1]] = value
    return result


# 사용 예시
if __name__ == "__main__":
    # 예시 1: 기본 사용법
    success = process_yaml_with_api(
        input_yaml_path="input.yaml",
        output_yaml_path="output.yaml",
        input_keys=["key1", "key2"],
        output_keys=["key1", "key2", "key3"],
        api_url="https://api.example.com/process",
        api_method="POST",
        api_headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer your-token-here"
        }
    )
    
    # 예시 2: 커스텀 요청 본문 템플릿 사용
    success = process_yaml_with_api(
        input_yaml_path="input.yaml",
        output_yaml_path="output.yaml",
        input_keys=["config", "message"],
        output_keys=["result", "processed", "data"],
        api_url="https://api.example.com/analyze",
        api_method="POST",
        request_body_template={
            "query": "{input_text}",
            "options": {
                "format": "json",
                "language": "ko"
            }
        }
    )
    
    print(f"처리 완료: {success}")


# 테스트를 위한 샘플 YAML 파일 생성 함수
def create_sample_files():
    """
    테스트용 샘플 파일들을 생성
    """
    # 샘플 입력 YAML
    sample_input = {
        "key1": {
            "key2": "안녕하세요, 이것은 테스트 텍스트입니다."
        },
        "config": {
            "message": "번역해주세요: Hello World"
        }
    }
    
    with open("sample_input.yaml", "w", encoding="utf-8") as f:
        yaml.dump(sample_input, f, default_flow_style=False, allow_unicode=True)
    
    print("샘플 파일 sample_input.yaml이 생성되었습니다.")


# 샘플 파일 생성 (필요시 주석 해제)
# create_sample_files()
