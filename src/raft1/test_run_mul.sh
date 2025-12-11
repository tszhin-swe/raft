> tmp2.txt

# run 1 test (or change 1..10 for 10 runs)
for i in $(seq 1 50); do
  echo "=== Run $i ===" | tee -a tmp2.txt
  stdbuf -oL go test -v -run 3C  | tee -a tmp2.txt
done