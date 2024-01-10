# syntax=docker/dockerfile:1.4
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements.txt /app
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install -r requirements.txt

COPY ./extract_rate_data.py /app/extract_rate_data.py

# ENTRYPOINT ["python3"]
CMD ["python3", "extract_rate_data.py"]

# FROM builder as dev-envs

# RUN <<EOF
# apk update
# apk add git bash
# EOF

# RUN <<EOF
# addgroup -S docker
# adduser -S --shell /bin/bash --ingroup docker vscode
# EOF
# # install Docker tools (cli, buildx, compose)
# COPY --from=gloursdocker/docker / /