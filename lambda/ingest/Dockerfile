FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies
RUN yum install -y \
    gcc \
    gcc-c++ \
    python3-devel \
    curl \
    && rm -rf /var/cache/yum/*

# Copy requirements
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

# Install Python dependencies
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy handler and modules
COPY handler.py ${LAMBDA_TASK_ROOT}/
COPY density_scorer.py ${LAMBDA_TASK_ROOT}/

# fastText .ftz model will be downloaded on first run and cached in /tmp

# Set the CMD to your handler
CMD [ "handler.lambda_handler" ]
