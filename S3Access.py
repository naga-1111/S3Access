import pandas as pd
import traceback
import json
import boto3
from typing import List

class S3Access:
  def __init__(self):
    """アカウント設定など
    https://qiita.com/tsukamoto/items/00ec8ef7e9a4ce4fb0e9
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    """
    self.client = boto3.client('s3',
        aws_access_key_id="",
        aws_secret_access_key="",
        region_name='ap-northeast-1')
    self.s3BucketName = ''
    self.s3path = 'OrderbookData10min/'
    return

  def s3_keys(self, buket:str, directory:str) -> List[str]:
    """s3のバケット名、パスから存在するファイル一覧を取得
    Args:
      bucket (str): バケット名
      folder (str): バケット内のディレクトリ名
    https://qiita.com/mynkit/items/76c1ecbdc0a9781917d5
    """

    def ls(bucket: str, prefix: str, recursive: bool = False) -> List[str]:
        """S3上のファイルリスト取得
        Args:
            bucket (str): バケット名
            prefix (str): バケット以降のパス
            recursive (bool): 再帰的にパスを取得するかどうか
        """
        paths: List[str] = []
        paths = __get_all_keys(
            bucket, prefix, recursive=recursive)
        return paths

    def __get_all_keys(bucket: str, prefix: str, keys: List = None, marker: str = '', recursive: bool = False) -> List[str]:
        """指定した prefix のすべての key の配列を返す
        Args:
            bucket (str): バケット名
            prefix (str): バケット以降のパス
            keys (List): 全パス取得用に用いる
            marker (str): 全パス取得用に用いる
            recursive (bool): 再帰的にパスを取得するかどうか
        """
        self.client
        if recursive:
            response = self.client.list_objects(
                Bucket=bucket, Prefix=prefix, Marker=marker)
        else:
            response = self.client.list_objects(
                Bucket=bucket, Prefix=prefix, Marker=marker, Delimiter='/')

        # keyがNoneのときは初期化
        if keys is None:
            keys = []

        if 'CommonPrefixes' in response:
            # Delimiterが'/'のときはフォルダがKeyに含まれない
            keys.extend([content['Prefix']
                        for content in response['CommonPrefixes']])
        if 'Contents' in response:  # 該当する key がないと response に 'Contents' が含まれない
            keys.extend([content['Key'] for content in response['Contents']])
            if 'IsTruncated' in response:
                return __get_all_keys(bucket=bucket, prefix=prefix, keys=keys, marker=keys[-1], recursive=recursive)
        return keys

    keys = ls(buket, directory, recursive=False)
    return keys

  def main(self):
    keys = self.s3_keys(self.s3BucketName, self.s3path)#s3の全ファイルを取得してくる、最後尾が最新ファイル
    path=keys[len(keys)-1]
    print(keys)
    print(path)

    try:
      response = self.client.get_object(Bucket=self.s3BucketName, Key=path)['Body'].read()
      res = response.decode('utf8').replace("'", '"')
      json_load = json.loads(res)
      print(json_load)
    except Exception:
      traceback.print_exc()
    return

if __name__ == "__main__":
  s3a = S3Access()
  s3a.main()
