# Rebooting

When rebooting the server on digital ocean, if app.regendata.xyz is not working, try the following steps:
1. Log into the console from the digital ocean dashboard.
2. Check the nginx status by running `sudo systemctl status nginx`
3. If the status is inactive, run `sudo systemctl start nginx`
4. Ensure nginx starts on boot by running `sudo systemctl enable nginx`. You can check what it's currently set to by running `sudo systemctl is-enabled nginx`

Nginx should now be running and the app should be accessible at app.regendata.xyz. If it's still not working, try rebooting the server by running `sudo reboot` and then check the status of nginx again. Nginx is a reverse proxy server that forwards requests to the app. 




            