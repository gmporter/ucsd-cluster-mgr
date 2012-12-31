#!/bin/bash

redis-cli keys  "*" | while read LINE ; do redis-cli del $LINE; done;
