import json
import pandas as pd
from typing import List, Dict


class YouTubeChannelTransform:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = self._process_data()
    
    def _load_json(self) -> List[Dict]:
        """Carrega os dados do arquivo JSON."""
        with open(self.file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    
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
            'playlist': channel.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads')
        }
    
    def _process_data(self) -> pd.DataFrame:
        """Carrega os dados, processa e retorna um DataFrame."""
        data = self._load_json()
        records = [self._extract_channel_info(channel) for channel in data]
        df = pd.DataFrame(records)
        df['dt_published'] = pd.to_datetime(df['dt_published'], format='ISO8601')
        return df
    
    def run(self, output_path: str):
        """Executa todo o processo e salva o DataFrame em CSV."""
        self.df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Data saved to {output_path}")
