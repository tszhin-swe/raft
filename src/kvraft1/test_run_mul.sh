> tmp2.txt

# run 1 test (or cha
for i in $(seq 1 20); do
  echo "=== Run $i ===" | tee -a tmp2.txt
  stdbuf -oL go test -v -run 4B  | tee -a tmp2.txt
done