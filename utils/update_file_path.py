import os
import pandas as pd
from pathlib import Path
from typing import Union

def update_file_paths(df: pd.DataFrame, 
                     target_col_name: str, 
                     target_file_dir: Union[str, Path], 
                     file_name_prefix: str) -> pd.DataFrame:
    """
    DataFrame의 test_idx 값에 해당하는 파일이 존재하면 해당 파일 경로를 target_col_name에 저장
    
    Parameters:
    -----------
    df : pd.DataFrame
        업데이트할 DataFrame (반드시 'test_idx' 컬럼이 있어야 함)
    target_col_name : str
        파일 경로를 저장할 컬럼명
    target_file_dir : str or Path
        파일을 검색할 디렉토리 경로
    file_name_prefix : str
        파일명 접두사 (예: "result_", "data_")
    
    Returns:
    --------
    pd.DataFrame
        업데이트된 DataFrame (원본 수정)
    
    Example:
    --------
    >>> df = pd.DataFrame({'test_idx': [1, 2, 3], 'file_path': [None, None, None]})
    >>> update_file_paths(df, 'file_path', '/data/files/', 'test_')
    # /data/files/test_1.txt, /data/files/test_3.csv 파일이 존재한다면
    # df['file_path']에 해당 경로들이 저장됨
    """
    
    # 입력 검증
    if 'test_idx' not in df.columns:
        raise ValueError("DataFrame에 'test_idx' 컬럼이 없습니다.")
    
    if target_col_name not in df.columns:
        raise ValueError(f"DataFrame에 '{target_col_name}' 컬럼이 없습니다.")
    
    # 디렉토리 경로 검증
    target_dir = Path(target_file_dir)
    if not target_dir.exists():
        raise FileNotFoundError(f"디렉토리가 존재하지 않습니다: {target_file_dir}")
    
    if not target_dir.is_dir():
        raise NotADirectoryError(f"경로가 디렉토리가 아닙니다: {target_file_dir}")
    
    # DataFrame 복사본 생성 (원본 보존을 원할 경우)
    # df_copy = df.copy()  # 필요시 주석 해제
    
    # 각 test_idx에 대해 파일 검색
    for idx, row in df.iterrows():
        test_id = row['test_idx']
        
        # 파일명 패턴 생성
        file_pattern = f"{file_name_prefix}{test_id}"
        
        # 디렉토리에서 매칭되는 파일 검색
        matching_files = []
        for file_path in target_dir.iterdir():
            if file_path.is_file() and file_path.stem.startswith(file_pattern):
                # 정확한 매칭 확인 (확장자 제외한 파일명이 정확히 일치하는지)
                if file_path.stem == file_pattern:
                    matching_files.append(file_path)
        
        # 매칭되는 파일이 있으면 경로 저장
        if matching_files:
            # 여러 파일이 있을 경우 첫 번째 파일 선택 (또는 특정 우선순위 적용 가능)
            selected_file = matching_files[0]
            df.at[idx, target_col_name] = str(selected_file.absolute())
            print(f"test_idx {test_id}: 파일 발견 - {selected_file}")
        else:
            print(f"test_idx {test_id}: 매칭되는 파일 없음 (패턴: {file_pattern}*)")
    
    return df


def update_file_paths_advanced(df: pd.DataFrame, 
                              target_col_name: str, 
                              target_file_dir: Union[str, Path], 
                              file_name_prefix: str,
                              file_extensions: list = None,
                              create_column: bool = True) -> pd.DataFrame:
    """
    고급 버전: 특정 확장자 필터링 및 컬럼 자동 생성 옵션 포함
    
    Parameters:
    -----------
    df : pd.DataFrame
        업데이트할 DataFrame
    target_col_name : str
        파일 경로를 저장할 컬럼명
    target_file_dir : str or Path
        파일을 검색할 디렉토리 경로
    file_name_prefix : str
        파일명 접두사
    file_extensions : list, optional
        허용할 파일 확장자 리스트 (예: ['.txt', '.csv', '.json'])
        None이면 모든 확장자 허용
    create_column : bool, default True
        target_col_name 컬럼이 없을 경우 자동 생성할지 여부
    
    Returns:
    --------
    pd.DataFrame
        업데이트된 DataFrame
    """
    
    # 입력 검증
    if 'test_idx' not in df.columns:
        raise ValueError("DataFrame에 'test_idx' 컬럼이 없습니다.")
    
    # 타겟 컬럼 존재 확인 및 생성
    if target_col_name not in df.columns:
        if create_column:
            df[target_col_name] = None
            print(f"'{target_col_name}' 컬럼을 새로 생성했습니다.")
        else:
            raise ValueError(f"DataFrame에 '{target_col_name}' 컬럼이 없습니다.")
    
    # 디렉토리 검증
    target_dir = Path(target_file_dir)
    if not target_dir.exists():
        raise FileNotFoundError(f"디렉토리가 존재하지 않습니다: {target_file_dir}")
    
    # 확장자 정규화
    if file_extensions:
        file_extensions = [ext.lower() if ext.startswith('.') else f'.{ext.lower()}' 
                          for ext in file_extensions]
    
    # 파일 매칭 및 업데이트
    updated_count = 0
    
    for idx, row in df.iterrows():
        test_id = row['test_idx']
        file_pattern = f"{file_name_prefix}{test_id}"
        
        # 매칭 파일 검색
        matching_files = []
        for file_path in target_dir.iterdir():
            if (file_path.is_file() and 
                file_path.stem == file_pattern and
                (not file_extensions or file_path.suffix.lower() in file_extensions)):
                matching_files.append(file_path)
        
        if matching_files:
            # 우선순위: 확장자 순서대로 또는 알파벳 순
            if file_extensions:
                # 지정된 확장자 순서대로 우선순위
                for ext in file_extensions:
                    for file_path in matching_files:
                        if file_path.suffix.lower() == ext:
                            df.at[idx, target_col_name] = str(file_path.absolute())
                            updated_count += 1
                            print(f"test_idx {test_id}: {file_path.name}")
                            break
                    else:
                        continue
                    break
            else:
                # 첫 번째 매칭 파일 사용
                selected_file = sorted(matching_files)[0]  # 알파벳 순 정렬
                df.at[idx, target_col_name] = str(selected_file.absolute())
                updated_count += 1
                print(f"test_idx {test_id}: {selected_file.name}")
    
    print(f"\n총 {updated_count}개 행이 업데이트되었습니다.")
    return df


# 사용 예시
if __name__ == "__main__":
    # 테스트 데이터 생성
    test_df = pd.DataFrame({
        'test_idx': [1, 2, 3, 4, 5],
        'description': ['test1', 'test2', 'test3', 'test4', 'test5'],
        'file_path': [None, None, None, None, None]
    })
    
    print("원본 DataFrame:")
    print(test_df)
    print("\n" + "="*50 + "\n")
    
    # 기본 함수 사용 예시
    try:
        # update_file_paths(test_df, 'file_path', './data/', 'result_')
        print("파일 경로 업데이트를 위해서는 실제 디렉토리와 파일이 필요합니다.")
        print("함수 호출 예시:")
        print("update_file_paths(df, 'file_path', '/path/to/files/', 'data_')")
        
    except Exception as e:
        print(f"오류: {e}")
    
    # 고급 함수 사용 예시
    print("\n고급 함수 사용법:")
    print("update_file_paths_advanced(df, 'file_path', './data/', 'result_', ['.csv', '.txt'])")
