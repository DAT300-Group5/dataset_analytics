#!/bin/bash
echo ""

echo "Before clearing cache:"
free -h

echo "Dropping caches..."
echo 3 > /proc/sys/vm/drop_caches

echo "After clearing cache:"
free -h
