set JAVA_HOME=C:\Program Files\Java\jdk-21
set PATH=%JAVA_HOME%\bin;%PATH%
cd /d D:\Nimbus\distributed-file-management
"D:\apache-maven-3.9.16\bin\mvn.cmd" clean test -DforkCount=0 -DreuseForks=false
