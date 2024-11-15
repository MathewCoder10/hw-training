import json
import datetime

class EmployeeTaskTracker:
    main_task_list = []  

    def __init__(self, emp_name, emp_id):
        self.emp_name = emp_name
        self.emp_id = emp_id
        self.login_time = None
        self.logout_time = None
        self.tasks = []  
        self.current_task = None 

    def login(self):
        self.login_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"{self.emp_name} logged in at {self.login_time}")

    def add_task(self, task_title, task_description):
        if not self.login_time:
            print("Please log in first!")
            return

        self.current_task = {
            "task_title": task_title,
            "task_description": task_description,
            "start_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "end_time": None,
            "task_success": None
        }

        print(f"Task '{task_title}' started at {self.current_task['start_time']}")

    def end_task(self, task_success=True):
        if self.current_task:
            self.current_task["end_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            self.current_task["task_success"] = task_success
            self.tasks.append(self.current_task)
            EmployeeTaskTracker.main_task_list.append(self.current_task)  
            print(f"Task '{self.current_task['task_title']}' ended at {self.current_task['end_time']}")
            self.current_task = None
        else:
            print("No task is currently being worked on.")

    def logout(self):
        if not self.login_time:
            print("Please log in first!")
            return

        self.logout_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

   
        workday_data = {
            "emp_name": self.emp_name,
            "emp_id": self.emp_id,
            "login_time": self.login_time,
            "logout_time": self.logout_time,
            "tasks": self.tasks
        }

        filename = f"{self.emp_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"


        with open(filename, 'w') as file:
            json.dump(workday_data, file, indent=4)
        
        print(f"Workday details saved to {filename}")
        self.tasks.clear()  
        self.login_time = None
        self.logout_time = None


emp1 = EmployeeTaskTracker(emp_name="Mathew George", emp_id=1)
emp1.login()
emp1.add_task("Task 1", "Employee Tracking Task finished")
emp1.end_task(task_success=True)
emp1.add_task("Task 2", "Employee Tracking Task doing")
emp1.end_task(task_success=True)
emp1.logout()

# emp2 = EmployeeTaskTracker(emp_name="George Mathew", emp_id=2)
# emp2.login()
# emp2.add_task("Task 1", "Employee Tracking Task finished")
# emp2.end_task(task_success=True)
# emp2.add_task("Task 2", "Employee Tracking Task doing")
# emp2.end_task(task_success= False)
# emp2.logout()


