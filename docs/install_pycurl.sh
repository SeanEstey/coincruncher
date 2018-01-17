export LDFLAGS="-L/usr/local/opt/openssl/lib"
export CPPFLAGS="-I/usr/local/opt/openssl/include"
export PKG_CONFIG_PATH="/usr/local/opt/openssl/lib/pkgconfig"
export PYCURL_SSL_LIBRARY=openssl
 
pip3 install pycurl --compile pycurl --no-cache
