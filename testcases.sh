#!/bin/bash
shopt -s extglob

rm PRINTCONTENTS
touch PRINTCONTENTS


get_num_hardlinks(){
    echo "$( ls -l | grep $1 | head -1 | cut -d " " -f2 )"
}

printall(){
    #../printcontents.py 2222 3333 >> PRINTCONTENTS
    echo fixme
}
#test=$(get_num_hardlinks fusemount)
#echo $test

#rm -r !(testcases*)

# number of tests
num_tests=0
tests_passed=0

# FOR TESTING PURPOSES
#mkdir fusemount 
cd fusemount

# TEST1: Write in a file "one.txt" and "four.txt"
string1="This is text for one.txt" # FIXME: load in file from text
string4="More text for four.txt"   # FIXME: load in file from text
echo $string1 > one.txt
echo $string4 >> four.txt
if [ "$( cat one.txt )" == "$string1" ] && [ "$( cat four.txt )" == "$string4" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 1 FAILED
fi
let num_tests+=1

# TEST2: Copy file "one.txt" to "two.txt"
cp one.txt two.txt
string2=$string1
if [ "$( cat two.txt )" == "$string2" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 2 FAILED
fi
let num_tests+=1

# TEST3: Move (Rename) file "two.txt" to "three.txt"
mv two.txt three.txt
string3=$string2
if [ -f three.txt ] && [ ! -f two.txt ]
    then 
        let tests_passed+=1
    else 
        echo TEST 3 FAILED
fi
let num_tests+=1

# TEST4: Read Renamed file "three.txt"
if [ "$( cat three.txt )" == "$string3" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 4 FAILED 
fi 
let num_tests+=1


# TEST5: Append file "one.txt"
append="...Appending to file one.txt"
echo $append >> one.txt
string1="$string1"$'\n'"$append"
if [ "$( cat one.txt )" == "$string1" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 5 FAILED 
fi 
let num_tests+=1


# TEST6: Check hardlinks of root before mkdir
cd ..
if [ "$( get_num_hardlinks fusemount )" == "2" ] # should have 2 hard links
    then
        let tests_passed+=1
    else 
        echo TEST 6 FAILED
fi
cd fusemount
let num_tests+=1

# TEST7: Create folder - mdkir -"folder_one"
mkdir folder_one
if [ -d folder_one ]
    then 
        let tests_passed+=1
    else 
        echo TEST 7 FAILED
fi
let num_tests+=1


# TEST8: Check No fo Hardlinks of parent ("/")
cd ..
if [ "$( get_num_hardlinks fusemount )" == "3" ] # should have 3 hard links
    then
        let tests_passed+=1
    else 
        echo TEST 8 FAILED
fi
cd fusemount
let num_tests+=1


# TEST9: move file "one.txt" to "folder_one"
mv one.txt folder_one
if [ ! -f one.txt ]
    then 
        let tests_passed+=1
    else 
        echo TEST 9 FAILED
fi
let num_tests+=1


# TEST10: check files in folder_one
if [ -f folder_one/one.txt ]
    then 
        let tests_passed+=1
    else 
        echo TEST 10 FAILED
fi
let num_tests+=1


# TEST11: read "one.txt" from "folder_one"
if [ "$( cat folder_one/one.txt )" == "$string1" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 11 FAILED 
fi 
let num_tests+=1

printall

# TEST12: create symlink for directory and file
ln -s folder_one/one.txt oneSYMLINK
#sleep .5
if [ "$( cat oneSYMLINK )" == "$string1" ] 
    then 
        let tests_passed+=1
    else 
        echo TEST 12 FAILED 
fi 
let num_tests+=1

printall

# TEST12DIR: create symlink for directory
ln -s folder_one/ folderSYMLINK
#sleep .5
if [ "$( ls folderSYMLINK )" == "$( ls folder_one )" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 12DIR FAILED 
fi 
let num_tests+=1

printall

# TEST13: create folder_two inside folder_one
mkdir folder_one/folder_two
if [ -d folder_one/folder_two ]
    then 
        let tests_passed+=1
    else 
        echo TEST 13 FAILED
fi
let num_tests+=1

# TEST14: Check hardlinks of parent (folder_one)
if [ "$( get_num_hardlinks folder_one )" == "3" ] # should have 2 hard links
    then
        let tests_passed+=1
    else 
        echo TEST 14 FAILED
fi
let num_tests+=1


# TEST15: Check hardlinks of new folder
cd folder_one
if [ "$( get_num_hardlinks folder_two )" == "2" ] # should have 2 hard links
    then
        let tests_passed+=1
    else 
        echo TEST 15 FAILED
fi
cd ..
let num_tests+=1

# TEST16: Remove folder_one FIXME: SHOULD IT BE REMOVE FOLDER 2???
rm folder_one 2>>/dev/null
if [ $? -eq 0 ]
    then
        echo TEST 16 FAILED
    else
        let tests_passed+=1
fi
let num_tests+=1

# TEST17: check number of hardlinks after remove
cd ..
if [ "$( get_num_hardlinks fusemount )" == "3" ] # should have 2 hard links
    then
        let tests_passed+=1
    else 
        echo TEST 17 FAILED
fi
cd fusemount
let num_tests+=1

# TEST18: copy "folder_one" to "folder_one_copy"
cp -r folder_one folder_one_copy
if [ -d folder_one_copy ] && [ "$( ls folder_one )" == "$( ls folder_one_copy )" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 18 FAILED
fi
let num_tests+=1

# TEST19: rmdir "folder_one_copy"
rm folder_one_copy 2>>/dev/null
if [ $? -eq 0 ]
    then
        echo TEST 19 FAILED
    else
        let tests_passed+=1
fi
let num_tests+=1

# TEST20: recursive delete "folder_one_copy"
rm -r folder_one_copy 
if [ ! -d folder_one_copy ]
    then 
        let tests_passed+=1
    else 
        echo TEST 20 FAILED
fi
let num_tests+=1

# TEST21: mkdir "folder_four" and move "folder_one" to inside "folder_four"
mkdir folder_four
mv folder_one folder_four/
if [ ! -d folder_one ] && [ -d folder_four/folder_one ]
    then 
        let tests_passed+=1
    else 
        echo TEST 21 FAILED
fi
let num_tests+=1

# TEST22: read "one.txt" from "/folder_four/folder_one"
if [ "$( cat folder_four/folder_one/one.txt )" == "$string1" ]
    then 
        let tests_passed+=1
    else 
        echo TEST 22 FAILED 
fi 
let num_tests+=1

# TEST23: remove "three.txt"
rm three.txt
if [ ! -f three.txt ]
    then 
        let tests_passed+=1
    else 
        echo TEST 23 FAILED 
fi 
let num_tests+=1

# TEST24: mkdir "st_mode"
mkdir st_mode
if [ -d st_mode ]
    then 
        let tests_passed+=1
    else 
        echo TEST 24 FAILED 
fi 
let num_tests+=1

# in directory fusemount
# delete everything created
# rm -r *
cd ..
echo $tests_passed/$num_tests tests passed