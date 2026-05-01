#!/usr/bin/env sh
set -eu

make clean
make
./spo_task2_v18 --self-test
./spo_task2_v18 --variant 48
./spo_task2_v18 --variant 78
