from datetime import datetime


def write_log(message):
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y %H:%M:%S")
    with open('../dblogs.log', 'a+', newline='') as file:
        file.write(current_time + " " + message + "\n")
    # print(current_time + " " + message)
    file.close()


if __name__ == "__main__":
    write_log("new msg")