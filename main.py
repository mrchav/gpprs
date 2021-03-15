import my
import time

if __name__ == '__main__':
    my.print_log('Старт проекта')
    my.connect()
    my.load_data()
    # start function once


    while True:

        # form tasks for spider
        my.form_new_tasks()



        # check task queue


        # spider working
        my.doing_new_tasks()

        #time.sleep(5)

        #update screens id
        my.update_id_in_sreens()
        #my.change_video_url()


