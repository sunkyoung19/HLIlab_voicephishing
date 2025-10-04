# HLIlab_voicephishing_익명화처리

## 원래 파일 실행 순서 (배치작업 지연 문제 없을 경우)
1. (전처리) **csv_json.py** 실행
- 입력 데이터 파일(vp_stt_labeled.csv, non_vishing_concat.csv)를 jsonl 파일로 변환
- vp_stt 데이터에서 hallucination 항목 제외
2. (배치 업로드) **deidentification.py** 실행
- custom_id에 원본 매핑 정보가 들어간 상태로 배치가 생성, 서버에 업로드
3. (서버 익명화 처리 진행률 확인) **watch_two_batches_tqdm.py (batch id 입력 필요)** 실행
- 익명화 작업 진행상황 확인을 위해 tqdm 으로 배치 프로세싱 과정 확인 가능
- 두 파일 모두 100% 완료 (completed) 상태 됐을 때 다음 단계 실행
4. (결과 불러오기) **outputcheck.py** 실행
  - OpenAI Batch의 output_file_id를 이용해서 결과 (result.jsonl) 다운로드
  - 필요 시 error file(errors.jsonl)도 같이 확인
5. (원보 포맷 복원) **UserCheck.py** 실행
- result.jsonl → custom_id 파싱 → 원본 CSV를 다시 열어 text만 교체
- 결과는 ./deid_csv_outputs/ 폴더에
    - vp_stt_labeled_deid.csv
    - non_vishing_concat_deid.csv로 저장
 
---
## 심한 지연 발생 경우 실행 순서 (작게 쪼개서 실행)
위의 1 & 2 & 3번 까지 실행 후 3에서 심한 지연 (진행률이 오랜시간 멈춰있을 경우) 발생할 경우

4. (해당 배치 취소) **cancel.py (멈출 배치 id 입력 필요)** 
5. (중단 상태 확인) **quick_inspect.py (확인할 배치 id 입력)** 
- canceling -> canceled 로 상태 바뀐 것 확인 후 아래 단계 실행
6. (실행 필요한 부분 잘라내기) **slice_jsonl.py**
- OpenAI Batch은 완료되기 전에는 부분 결과를 내려주지 않기 때문에 이전까지 진행된 상태를 확인 후 마지막 15% 정도만 다시 돌리는 것이 안전
- 콘솔에 "python slice_jsonl.py ./sms/vp_stt_labeled.jsonl ./sms/vp_stt_labeled_tail.jsonl 1288" 이런 형식으로 입력
- vp_stt_labeled_tail.jsonl 파일 생성
7. (필요 부분만 다시 배치 업로드) **run_tail_vp_stt.py (slice 된 파일명 입력)**
8. (진행상태 확인) quick_inspect.py / watch_two_batches_tqdm.py 중 편한 것 선택하여 배치 id 입력 후 확인
- completed 됐을 경우 outputcheck.py 실행하여 결과 파일 불러오기
- 원할 경우 부분 결과/에러 파일 받아두기 save_partial_vpstt.py
9. 여전히 나머지 배치에서 심한 지연이 발생할 경우 -> 나머지 배치를 반으로 쪼갬 
- 기존 실행 중이었던 파일 작업 중단 **(cancel.py)**
- 두개의 나눠진 파일 각각 생성 "python **slice_jsonl.py** ./sms/vp_stt_labeled_tail.jsonl ./sms/vp_stt_labeled_tail_A17.jsonl 0 17",
"python slice_jsonl.py ./sms/vp_stt_labeled_tail.jsonl ./sms/vp_stt_labeled_tail_B17.jsonl 17 34"
10. (분리된 배치 업로드) **run_tail_generic.py**
- python run_tail_generic.py ./sms/vp_stt_labeled_tail_A17.jsonl (실행 파일 각각 업로드)
11. (두 개의 파일로 결과 불러오기) **split_merge_results.py (배치id 입력)**
-  각 배치의 output_file_id를 내려받아 batch_results/에 저장
-  두 그룹(non_vishing, vp_stt)으로 나누어 병합 + 중복 제거(custom_id 기준)
-  그룹별로 생성되는 파일
    - *_merged.jsonl : 배치 원본 응답 라인 그대로
    - *_deid_only.jsonl : 익명화 텍스트만
    - *_deid_outputs.csv : CSV (컬럼: custom_id,deidentified_text)
12. (원본포맷으로 csv 파일생성) **patch_texts_from_deid.py**
- 원본 csv 불러오고, custom_id → original_index 매핑해서 익명화 텍스트 적용 -> CSV 전체 저장
- ./deid_csv_outputs/ 안에 non_vishing_concat_deid.csv, vp_stt_labeled_deid.csv 익명화된 csv 파일 저장
