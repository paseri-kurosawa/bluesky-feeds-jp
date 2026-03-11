FROM public.ecr.aws/lambda/python:3.11

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy handler
COPY handler.py .

# Set the CMD to your handler
CMD [ "handler.lambda_handler" ]
