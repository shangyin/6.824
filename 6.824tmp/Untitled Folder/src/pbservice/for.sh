rm log
for i in {1..20}
do 
    echo "start" >> log
    go test >> log
    echo "end" >> log
    echo "" >> log
    echo "" >> log
done
