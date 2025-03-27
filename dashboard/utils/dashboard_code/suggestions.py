import pandas as pd
import streamlit as st
from io import BytesIO
from utils.dashboard_code.texts import (
    TITLE_SUGGEST_CHANNEL, INPUT_SUGGEST_CHANNEL, BTN_SEND_SUGGESTION, MSG_SUGGESTION_SUCCESS, MSG_SUGGESTION_EMPTY
)

def save_suggestion_to_parquet(s3_client, channel_suggestion, dashboard, bucket_name, s3_key_suggestions):
    """
    Salva a sugestão do usuário em um arquivo .parquet no S3.
    
    Se o arquivo de sugestões existir, a nova sugestão será adicionada; caso contrário, será criado um novo arquivo.
    """
    suggestion = {"channel_name": channel_suggestion, "dashboard": dashboard}
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key_suggestions)
        data = response['Body'].read()
        existing_df = pd.read_parquet(BytesIO(data))
    except Exception as e:
        existing_df = pd.DataFrame(columns=["channel_name", "dashboard"])
    
    new_df = pd.DataFrame([suggestion])
    updated_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    out_buffer = BytesIO()
    updated_df.to_parquet(out_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=s3_key_suggestions, Body=out_buffer.getvalue())
    
    st.success(MSG_SUGGESTION_SUCCESS.format(channel_suggestion))


def suggest_channel(s3_client, dashboard, bucket_name, s3_key_suggestions):
    st.title(TITLE_SUGGEST_CHANNEL)
    channel_suggestion = st.text_input(INPUT_SUGGEST_CHANNEL)
    if st.button(BTN_SEND_SUGGESTION):
        if channel_suggestion:
            save_suggestion_to_parquet(s3_client, channel_suggestion, dashboard, bucket_name, s3_key_suggestions)
        else:
            st.warning(MSG_SUGGESTION_EMPTY)