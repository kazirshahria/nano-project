FROM public.ecr.aws/lambda/python:3.12

ENV DB_HOST= \
    DB_PORT= \
    DB_USER= \
    DB_PASSWORD= \
    DB_NAME= \
    AWS_ACCESS_KEY_ID= \
    AWS_SECRET_ACCESS_KEY= \
    GSHEET_URL=

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

COPY google_credentials.json ${LAMBDA_TASK_ROOT}
COPY src/ ${LAMBDA_TASK_ROOT}

CMD ["lambda_handler.handler"]
