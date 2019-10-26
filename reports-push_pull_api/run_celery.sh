#!/usr/bin/env bash
#server command path
/var/www/html/venv/bin/celery worker -A celery_config -l info
#/home/alonzo/apps/env/bin/celery worker -A celery_config -l info
