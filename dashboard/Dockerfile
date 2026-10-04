FROM python:3.11-slim
RUN pip install --no-cache-dir streamlit==1.40.2 cassandra-driver==3.29.2
WORKDIR /app
COPY .streamlit .streamlit
COPY app.py queries.py chart.py ./
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
