import json
import pandas as pd
import boto3
from typing import List, Dict
from io import BytesIO


class YouTubeChannelTransform:
    def __init__(self, bucket_name: str, s3_key: str, output_s3_key: str):
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        self.output_s3_key = output_s3_key
        self.s3_client = boto3.client("s3")
        self.df = self._process_data()
        self.run()
    
    def _load_json_from_s3(self) -> List[Dict]:
        """Carrega os dados JSON diretamente do S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_key)
            json_data = response['Body'].read().decode('utf-8')
            return json.loads(json_data)
        except Exception as e:
            print(f"Erro ao carregar o arquivo JSON do S3: {e}")
            return []
    
    def _extract_channel_info(self, channel: Dict) -> Dict:
        """Extrai e processa as informações relevantes de um canal."""
        return {
            'channel_id': channel.get('id'),
            'channel_name': channel.get('snippet', {}).get('title'),
            'custom_url': channel.get('snippet', {}).get('customUrl'),
            'dt_published': channel.get('snippet', {}).get('publishedAt'),
            'country': channel.get('snippet', {}).get('country'),
            'total_view': int(channel.get('statistics', {}).get('viewCount', 0)),
            'total_subscriber': int(channel.get('statistics', {}).get('subscriberCount', 0)),
            'total_video': int(channel.get('statistics', {}).get('videoCount', 0)),
            'playlist': channel.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads'),
            'dashboard': channel.get('dashboard', '')
        }
    
    def _process_data(self) -> pd.DataFrame:
        """Carrega os dados, processa e retorna um DataFrame."""
        data = self._load_json_from_s3()
        records = [self._extract_channel_info(channel) for channel in data]
        df = pd.DataFrame(records)
        df['dt_published'] = pd.to_datetime(df['dt_published'], format='ISO8601')
        return df

    def save_to_s3_parquet(self):
        """Converte o DataFrame para Parquet e salva no S3."""
        # Usando o to_parquet para salvar o DataFrame diretamente no formato Parquet
        try:
            # Usando um buffer de memória para evitar a criação de um arquivo local
            parquet_buffer = BytesIO()
            
            # Salvando o DataFrame no buffer em formato Parquet
            self.df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            
            # Reposicionando o cursor do buffer
            parquet_buffer.seek(0)

            # Enviando o arquivo Parquet para o S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.output_s3_key,
                Body=parquet_buffer,
                ContentType="application/octet-stream"
            )
            print(f"Data successfully uploaded to s3://{self.bucket_name}/{self.output_s3_key}")
        except Exception as e:
            print(f"Error uploading to S3: {e}")
    
    def run(self):
        """Executa o processo e salva os dados no S3 em formato Parquet."""
        self.save_to_s3_parquet()
