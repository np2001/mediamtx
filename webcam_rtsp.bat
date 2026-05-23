@echo off
:loop
echo Run camera...
ffmpeg -f dshow -rtbufsize 100M -i video="2K FHD camera" -c:v libx264 -preset ultrafast -tune zerolatency -b:v 2000k -g 3 -f rtsp -rtsp_transport tcp rtsp://127.0.0.1:8554/cam
echo Restarting camera...
timeout /t 3 /nobreak >nul
goto loop

