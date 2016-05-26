import subprocess

ipFile = "/Users/mayank.mahajan/Downloads/patch_installer_and_rollback/new-nrmca-ips"

with open(ipFile,'r') as ipFile:
    for line in ipFile:
        cmd = "ping -c 2 " + line.strip('\n')
        try:
            subprocess.check_output(cmd,shell=True)
        except subprocess.CalledProcessError as e:
            print '**************', e.output
