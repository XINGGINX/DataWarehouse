# Module import 
import getpass

# helper files
import controller
import helper

# section boarder 
section_start_log = "-" * 90
section_finish_log = section_start_log + "\n"

#=========================================================
#                   Main function
#=========================================================
if __name__ == "__main__":
    print(section_start_log)

    conf = helper.read_config("./config/conf.json")

    # asking password for 3 times
    for tolerance in range(0, 3):
        print("- main(): ENTER PASSWORD ...")

        # asking for password and verify 
        password = getpass.getpass()
        unlock = helper.create_connection(password, conf, log = False)

        # Stop early if password is correct
        if (unlock != False):
            break
        else:
            print("- main(): PASSWORD INCORRECT ... \n")


    # using controller to get command 
    if(unlock != False):
        print(section_start_log)
        print("- main(): STARTING ... \n\n")
        controller.controller(password)
    else:
        print("- main(): MAX ATTEMPT EXCEEDED ...")
        print(section_finish_log)
        print(40*"\n")
        exit(0)
