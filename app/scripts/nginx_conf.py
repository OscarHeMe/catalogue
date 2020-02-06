# -*- coding: utf-8  -*-
""" 
NginX Config generator script

It generates the correct path of the nginx 
server block, with correspondent `route` and `region`.

Must be executed as follows from root directory:

python -m app.scripts.nginx_conf REGION SERVICE_ROUTE

REGION :: Specify region of deployment
SERVICE_ROUTE :: Specify route of deployment

"""
import sys

nginx_block = """server {
    listen 80;
    server_name _;
    charset utf-8;
    location /%s/%s {
        rewrite ^/%s/%s/(.*)$ /$1 break;
        include proxy_params;
        proxy_pass http://unix:/catalogue/catalogue.sock;
        proxy_set_header X-Script-Name /bpcatalogue;
        proxy_read_timeout 500;
        proxy_connect_timeout 500;
        proxy_send_timeout 500;
        send_timeout 500;

    }
}
"""

nginx_block_def = """server {
    listen 80;
    server_name _;
    charset utf-8;
    location /%s/ {
        rewrite ^/%s/(.*)$ /$1 break;
        include proxy_params;
        proxy_pass http://unix:/catalogue/catalogue.sock;
        proxy_set_header X-Script-Name /bpcatalogue;
        proxy_read_timeout 500;
        proxy_connect_timeout 500;
        proxy_send_timeout 500;
        send_timeout 500;

    }
}
"""

if __name__ == '__main__':
    # Passing arguments validation
    if len(sys.argv) < 3:
        raise Exception("Missing Parameters [REGION] [SERVICE_ROUTE]")
    with open('cfn/nginx/conf.d/default.conf', 'w') as ngxfile:
        if str(sys.argv[1]):
            # For Region Set
            block_fmted = nginx_block % (
                str(sys.argv[1]).lower(), # REGION
                str(sys.argv[2]).lower(), # SERVICE_ROUTE
                str(sys.argv[1]).lower(), # REGION
                str(sys.argv[2]).lower() # SERVICE_ROUTE
            )
        else:
            # For default Set
            block_fmted = nginx_block_def % (
                str(sys.argv[2]).lower(), # SERVICE_ROUTE
                str(sys.argv[2]).lower() # SERVICE_ROUTE
            )
        # Writing File
        ngxfile.write(block_fmted)
    print("Correctly Generated config file for [{}] [{}]"\
        .format(sys.argv[1], sys.argv[2]))