#!/bin/bash
#######
#going over all the file in the dir, change the name and move them to json_dir
#######
dir_name="tweetJson"

input_path=${1}
# some comment remove this line, only or test reasons
NLP=${2}

if [ -z "${input_path}" ]; then
    echo "[Error] no path was given"
    exit
fi

cur_path=${input_path}
#loop over all the files in the target dir
#change the name and extract

if [ ! -d ${cur_path}${dir_name} ]; then
    mkdir "${cur_path}${dir_name}"
fi

for f in "$cur_path"*; do
    
    if [[ -f $f ]]; then
        new_name=$(basename "$f")
        #echo '----' 
        #echo "val="${new_name}
        IFS='=' read -ra ADDR <<< "$new_name"
        for i in "${ADDR[@]}"; do
            case $i in *.gz)
            #echo $i
            b=${i%_*}
            suffix=".json.gz_"
            #echo "f="${f}
            rel_path=${f%${suffix}*}
            mv ${f} ${rel_path}"_"${b}".json.gz"
            #mv "$f" "${f%%_h.png}_half.png"
            tar
            ;; esac 
        done
    fi
done | awk 'END { printf("File count: %d", NR); } NF=NF'

for f in "$cur_path"*; do
    if [[ -f $f ]]; then
        if [[ $f == *.gz ]]; then
            gzip -d $f 
        fi
    fi
done

for f in "$cur_path"*; do
    if [[ -f $f ]]; then
        if [[ $f == *.json ]];then
            mv  $f ${cur_path}${dir_name}/
        fi
    fi
done

python ${NLP}/parser_2015.py fix ${cur_path}${dir_name}
