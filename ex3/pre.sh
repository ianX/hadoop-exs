#!/bin/bash
if [ $# -lt 3 ]
then
    echo "usage: ./pre.sh <indir> <outdir> <prefix> [<mergeUnit>]"
    exit
fi
indir=$1;
prefix=$2/$3
if [ -n "$4" ]
then
    mergeUnit=$4
else
    mergeUnit=400
fi
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
    fi
    awk -F'[:,]' '{if(FNR==1){printf("%s\t",$1)}else if(FNR==2){printf("%s,%s",$1,$2)}else{printf("/%s,%s",$1,$2)}}END{printf("\n")}' $path >>$prefix-$outnum.txt
    let "count=$count+1"
done
