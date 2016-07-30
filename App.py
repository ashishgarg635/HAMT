#!/usr/bin/python2


from os import system
import commands

def extract(filename):
    file = open(filename,'r')
    data = file.read()
    file.close()
    return data


commands.getoutput('mkdir temp')
print commands.getoutput('yum install dialog -y')
system("dialog  --backtitle  ' My Project ' --title  ' Hadoop App '  --msgbox  '\\n\\n               Welcome to Hadoop Client App \\n\\n\\n     This app helps you perform operations over Hadoop cluster without running any commands instead providing a menu to select from. You can perform only a selected number of operations as all the hadoop operations are not featured here. '  15   60")
choice = '11'
while choice != '0' and choice != '' :
    system("dialog  --backtitle  ' My Project ' --title  ' Hadoop App '   --menu    ' \\n\\n Choose an operation : \\n '  20  40  10   1  'Put file'   2 'List files' 3  'Read a file' 4 'Make a directory'  5  'Delete a directory'   6  'Generate report'  7  'Run a job'  8  'Switch Safemode'   9  'Job list '   0   'Quit'  2> temp/mainoption.out")
    choice = extract('temp/mainoption.out')
    if choice == '1':
        system("dialog  --backtitle  ' My Project ' --title  ' Put a file '   --fselect  '/'   20  60  2>  temp/source.out ")
        source = extract('temp/source.out')
        system('''dialog  --backtitle  ' My Project ' --title  ' Put a file '   --inputbox  '\\nEnter the destiantion :'   10  40  2> temp/destination.out ''')
        destination = extract('temp/destination.out')
        x,y = commands.getstatusoutput('hadoop  fs   -put  '+source+'   '+destination)
        if x == 0:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Put a file '   --msgbox  '\\n  Result :  \\n\\n        Operation sucessfull !!!'   10  40    ''')
        else:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Put a file '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif choice == '2':
        system(''' dialog  --backtitle  ' My Project ' --title  ' List files '   --inputbox  '\\nEnter a directory :'   10  40  2> temp/directory.out  ''')
        directory = extract('temp/directory.out')
        x,y = commands.getstatusoutput('hadoop  fs  -ls   '+directory)
        if x == 0:
            file = open('temp/scratch.txt','w')
            file.write(y)
            file.close()
            system(''' dialog  --backtitle  ' My Project ' --title  ' List files '   --textbox    'temp/scratch.txt'   20  100   ''')
        else:
            system(''' dialog  --backtitle  ' My Project ' --title  ' List files '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif choice == '3':
        system(''' dialog  --backtitle  ' My Project ' --title  ' Read file '   --inputbox  '\\nEnter the absolute path of the file :'   10  50  2> temp/file.out  ''')
        source = extract('temp/file.out')
        x,y = commands.getstatusoutput(' hadoop fs -cat   ' + source )
        if x == 0:
            file = open('temp/scratch.txt', 'w')
            file.write(y)
            file.close()
            system(''' dialog  --backtitle  ' My Project ' --title  ' Read file '   --textbox    'temp/scratch.txt'   20  100   ''')
        else:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Read file '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif  choice == '4':
        system(''' dialog  --backtitle  ' My Project ' --title  ' Make directory '   --inputbox  '\\nEnter name of directory (absolute path) :'   10  50  2> temp/directory.out  ''')
        directory = extract('temp/directory.out')
        x, y = commands.getstatusoutput('hadoop  fs  -mkdir   ' + directory)
        if x==0:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Make directory '   --msgbox  '\\n  Result :  \\n\\n        Directory have been created !!!'   10  50    ''')
        else:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Make directory '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif  choice == '5':
        system(''' dialog  --backtitle  ' My Project ' --title  ' Remove directory '   --inputbox  '\\nEnter name of directory (absolute path) :'   10  50  2> temp/directory.out  ''')
        directory = extract('temp/directory.out')
        x, y = commands.getstatusoutput('hadoop  fs  -rmdir   ' + directory)
        if x == 0:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Remove directory '   --msgbox  '\\n  Result :  \\n\\n        Directory have been deleted !!!'   10  50    ''')
        else:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Remove directory '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif choice == '6':
        x, y = commands.getstatusoutput('hadoop  dfsadmin  -report ')
        if x == 0:
            file = open('temp/scratch.txt', 'w')
            file.write(y)
            file.close()
            system(''' dialog  --backtitle  ' My Project ' --title  ' Generate report '   --textbox    'temp/scratch.txt'   30  80   ''')
        else:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Generate report '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif choice == '7':
        system(''' dialog  --backtitle  ' My Project ' --title  ' Run a job '   --inputbox  '\\nEnter operation name :'   10  50  2> temp/operation.out  ''')
        operation = extract('temp/operation.out')
        system(''' dialog  --backtitle  ' My Project ' --title  ' Run a job '   --inputbox  '\\nEnter name of input file (absolute path) :'   10  50  2> temp/input.out  ''')
        infile = extract('temp/input.out')
        system(''' dialog  --backtitle  ' My Project ' --title  ' Run a job '   --inputbox  '\\nEnter output directory (absolute path) :'   10  50  2> temp/output.out  ''')
        outputdir = extract('temp/output.out')
        status, version = commands.getstatusoutput(' hadoop version | grep Hadoop | cut -d " " -f 2 | cut -d "."  -f 1')
        string = ' '
        if version == '1':
            string += ' hadoop jar /usr/share/hadoop/hadoop-examples-1.2.1.jar    '
        elif  version =='2':
            string += ' yarn jar /H2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar    '
        x,y = commands.getstatusoutput(string+operation+'   '+infile+ '   '+ outputdir)
        print x,y
        if x == 0:
            file = open('temp/scratch.txt', 'w')
            file.write(y)
            file.close()
            system(''' dialog  --backtitle  ' My Project ' --title  ' Run a job '   --textbox    'temp/scratch.txt'   30  80   ''')
        else:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Run a job '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')


    elif choice == '8':
        status, version = commands.getstatusoutput(' hadoop version | grep Hadoop | cut -d " " -f 2 | cut -d "."  -f 1')
        string = ' '
        if version == '1':
            string += 'hadoop dfsadmin  -safemode  '
        elif version == '2':
            string += 'hdfs  dfsadmin  -safemode  '
        current = commands.getoutput(string + '  get ')
        system(''' dialog  --backtitle  ' My Project ' --title  ' Switch Safemode '   --menu  ' \\n\\n ''' + current + ''' \\n\\n Choose a mode : ' 15  50  2  1   ' Enter safemode '  2  ' Leave safemode ' 2> temp/safemode.out  ''')
        safemode = extract('temp/safemode.out')
        ch = ''
        if safemode == '1':
            ch = 'enter'
        elif safemode == '2':
            ch = 'leave'
        x,y = commands.getstatusoutput(string+ch)
        print x,y
        if x == 0:
            system(''' dialog  --backtitle  ' My Project ' --title  ' Switch Safemode '   --msgbox  '\\n  Result :  \\n\\n        Operation sucessfull !!!'   10  40    ''')
        else:
            print y
            system(''' dialog  --backtitle  ' My Project ' --title  ' Switch Safemode '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif choice == '9':
        status, version = commands.getstatusoutput(' hadoop version | grep Hadoop | cut -d " " -f 2 | cut -d "."  -f 1')
        string = ' '
        if version == '1':
            string += 'hadoop job -list all'
        elif version == '2':
            string += 'yarn application -list'
        x,y = commands.getstatusoutput(string)
        print x,y
        if x == 0:
            file = open('temp/scratch.txt', 'w')
            file.write(y)
            file.close()
            system(''' dialog  --backtitle  ' My Project ' --title  ' Job list '   --textbox    'temp/scratch.txt'   30  80   ''')
        else:
            print y
            system(''' dialog  --backtitle  ' My Project ' --title  ' Job list '   --msgbox  '\\n  Result :  \\n\\n        Operation unsucessfull !!!'   10  40    ''')

    elif choice == '0' :
        system(''' dialog  --backtitle  ' My Project ' --title  ' Thanks giving '   --infobox  ' \\n\\n\\n             Bye ... '   10  40    ''')
        break





