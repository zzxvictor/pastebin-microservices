FROM haproxy:2.3
COPY haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg
CMD ["sh", "-c", "haproxy -d -f /usr/local/etc/haproxy/haproxy.cfg"]