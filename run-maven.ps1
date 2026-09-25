$env:JAVA_HOME='C:\Program Files\Java\jdk-21'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
Set-Location 'D:\Nimbus\distributed-file-management'
mvn clean test -DforkCount=0 -DreuseForks=false
