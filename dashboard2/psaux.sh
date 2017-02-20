#!/bin/bash
ps aux | grep 'start.sh'   | grep -v 'grep'
ps aux | grep 'python ojm' | grep -v 'grep'

