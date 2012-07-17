#!/bin/bash
indir=$1;

prefix=$2/$3

mergeUnit=400;
count=0;
outnum=0;

files=`ls $indir`
for file in $files
do
    echo processing $file
    path=$indir/$file
    if [ $count -eq $mergeUnit ] 
    then
        count=0
        let "outnum=$outnum+1"
    else
        awk -F'[:,]' '{if(FNR==1){printf("%s\t",$1)}else if(FNR==2){printf("%s,%s",$1,$2)}else{printf("/%s,%s",$1,$2)}}END{printf("\n")}' $path >>$prefix-$outnum.txt
    fi
    let "count=$count+1"
done
