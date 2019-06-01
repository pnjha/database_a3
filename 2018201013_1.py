import copy
import os
import sys
import re

output_filename = "2018201013_1.txt"

def create_data_dictionary(file_line):

	element_list = file_line.split(" ")

	data_dict_disk = {}
	data_dict_memeory = {}

	for i in range(0,len(element_list),2):

		data_dict_disk[element_list[i]] = int(element_list[i+1])
		data_dict_memeory[element_list[i]] = None
	
	#print(data_dict_disk)
	#print(data_dict_memeory)

	return data_dict_disk,data_dict_memeory

def is_all_transaction_over(transaction_instruction_counter,transaction_dict):

	counter = 0
	for key,values in transaction_dict.items():
		# #print(transaction_instruction_counter[counter] ,len(values))
		if transaction_instruction_counter[counter] < len(values):

			return False

		counter += 1

	return True

def perform_operation(instruction,value1,value2):

	if "+" in instruction:
		return value1 + value2
	elif "-" in instruction:
		return value1 - value2
	elif "/" in instruction:
		return value1 / value2
	elif "*" in instruction:
		return value1 * value2

def all_output_executed(instruction,transaction_dict,transaction_id):

	all_instuction_list = transaction_dict[transaction_id]

	check_flag = False
	counter = 1

	for inst in all_instuction_list:

		if inst == instruction:
			check_flag = True
			if counter == len(all_instuction_list):
				return True


		counter += 1
	return False

def execute_instruction(params):

	instruction,data_elements_on_disk,data_elements_in_memory,transaction_dict,transaction_id,temporary_var_dict = params

	transaction_id = transaction_id.strip()	

	if "READ" in instruction:
		operation, data_element, temporary_var = re.split("\(|\)|,",instruction)[:3]

		operation = operation.strip() 
		data_element = data_element.strip()
		temporary_var = temporary_var.strip()

		if data_elements_in_memory[data_element] == None:	
			temporary_var_dict[temporary_var] = data_elements_on_disk[data_element]		
			data_elements_in_memory[data_element] = data_elements_on_disk[data_element]

		else:
			temporary_var_dict[temporary_var] = data_elements_in_memory[data_element]

	elif "WRITE" in instruction:
		operation, data_element, temporary_var = re.split("\(|\)|,",instruction)[:3]

		operation = operation.strip() 
		data_element = data_element.strip()
		temporary_var = temporary_var.strip()
		old_value_data_element = data_elements_in_memory[data_element]
		data_elements_in_memory[data_element] = temporary_var_dict[temporary_var] 


		#print("data_elements_in_memory: ",data_elements_in_memory)
		#print("data_elements_on_disk: ",data_elements_on_disk)
		#print()


		output_flie = open(output_filename,"a")
		log_record = "<" + transaction_id + ", "+ data_element + ", " + str(old_value_data_element) + ">\n"
		output_flie.write(log_record)

		log_record = ""

		for key in sorted (data_elements_in_memory.keys()):
			if data_elements_in_memory[key] != None:
				log_record += key + " " + str(data_elements_in_memory[key]) + " "

		log_record = log_record.strip()
		log_record += "\n"
		output_flie.write(log_record)

		log_record = ""

		for key in sorted (data_elements_on_disk.keys()):
			if data_elements_on_disk[key] != None:
				log_record += key + " " + str(data_elements_on_disk[key]) + " "

		log_record = log_record.strip()
		log_record += "\n"
		output_flie.write(log_record)
				

		output_flie.close()

	elif "OUTPUT" in instruction:
		operation, data_element = re.split("\(|\)|,",instruction)[:2]

		operation = operation.strip() 
		data_element = data_element.strip()

		data_elements_on_disk[data_element] = data_elements_in_memory[data_element]

		# if all_output_executed(instruction,transaction_dict,transaction_id):

			# #print("data_elements_in_memory: ",data_elements_in_memory)
			# #print("data_elements_on_disk: ",data_elements_on_disk)
			# #print()

			# output_flie = open(output_filename,"a")
			# log_record = "<" + "COMMIT " + transaction_id + ">\n"
			# output_flie.write(log_record)

			# log_record = ""

			# for key in sorted (data_elements_in_memory.keys()):
			# 	if data_elements_in_memory[key] != None:
			# 		log_record += key + " " + str(data_elements_in_memory[key]) + " "  

			# log_record = log_record.strip()
			# log_record += "\n"
			# output_flie.write(log_record)

			# log_record = ""

			# for key in sorted (data_elements_on_disk.keys()):
			# 	if data_elements_on_disk[key] != None:
			# 		log_record += key + " " + str(data_elements_on_disk[key]) + " "

			# log_record = log_record.strip()
			# log_record += "\n"
			# output_flie.write(log_record)
					
			
			# output_flie.close()

	else:
		lvalue, operand1, operand2 = re.split(":=|\*|\/|\+|\-",instruction)

		lvalue = lvalue.strip()
		operand1 = operand1.strip()
		operand2 = operand2.strip()		

		try:
			operand2 = int(operand2)
		except ValueError:
			pass			

		try:
			operand1 = int(operand1)
		except ValueError:
			pass

		if type(operand1) == str and type(operand2) == str:
			temporary_var_dict[lvalue] = perform_operation(instruction,temporary_var_dict[operand1],temporary_var_dict[operand2])
		elif type(operand1) == str and type(operand2) == int:
			temporary_var_dict[lvalue] = perform_operation(instruction,temporary_var_dict[operand1], operand2)
		elif type(operand1) == int and type(operand2) == str:
			temporary_var_dict[lvalue] = perform_operation(instruction,temporary_var_dict[operand2], operand1)
		elif type(operand1) == int and type(operand2) == int:
			temporary_var_dict[lvalue] = perform_operation(instruction,operand1, operand2)

	return data_elements_on_disk,data_elements_in_memory,temporary_var_dict

def generate_undo_log(round_robin,data_elements_on_disk,data_elements_in_memory,transaction_dict):

	transaction_instruction_counter = [0]*len(transaction_dict)
	
	temporary_var_dict = {}
	
	# for key,values in transaction_dict.items():
	# 	temporary_var_dict[key] = {}

	while is_all_transaction_over(transaction_instruction_counter,transaction_dict) == False:

		counter = 0

		for key,values in transaction_dict.items():
			#print("key: ",key )
			transaction_id = key

			if transaction_instruction_counter[counter] == 0:
				log_record = ""
				output_flie = open(output_filename,"a")
				log_record = "<" + "START " + key + ">\n"
				output_flie.write(log_record)
				
				log_record = ""

				for item in sorted (data_elements_in_memory.keys()):
					if data_elements_in_memory[item] != None:
						log_record += item + " " + str(data_elements_in_memory[item]) + " "

				log_record = log_record.strip()
				log_record += "\n"
				output_flie.write(log_record)

				log_record = ""

				for item in sorted (data_elements_on_disk.keys()):
					if data_elements_on_disk[item] != None:
						log_record += item + " " + str(data_elements_on_disk[item]) + " "

				log_record = log_record.strip()
				log_record += "\n"
				output_flie.write(log_record)


				output_flie.close()
			
			for i in range(round_robin):
				if transaction_instruction_counter[counter] < len(values):

					params = [values[transaction_instruction_counter[counter]],data_elements_on_disk,data_elements_in_memory,transaction_dict,key,temporary_var_dict]
					data_elements_on_disk,data_elements_in_memory,temporary_var_dict = execute_instruction(params)
					transaction_instruction_counter[counter] += 1
				
				if transaction_instruction_counter[counter] == len(values):
					
					transaction_instruction_counter[counter] += 1

					#print("Completed: ",transaction_id)

					#print("data_elements_in_memory: ",data_elements_in_memory)
					#print("data_elements_on_disk: ",data_elements_on_disk)
					#print()

					output_flie = open(output_filename,"a")
					log_record = "<" + "COMMIT " + transaction_id + ">\n"
					output_flie.write(log_record)

					log_record = ""

					for key in sorted (data_elements_in_memory.keys()):
						if data_elements_in_memory[key] != None:
							log_record += key + " " + str(data_elements_in_memory[key]) + " "  

					log_record = log_record.strip()
					log_record += "\n"
					output_flie.write(log_record)

					log_record = ""

					for key in sorted (data_elements_on_disk.keys()):
						if data_elements_on_disk[key] != None:
							log_record += key + " " + str(data_elements_on_disk[key]) + " "

					log_record = log_record.strip()
					log_record += "\n"
					output_flie.write(log_record)
							
					
					output_flie.close()


			counter += 1

	# #print(temporary_var_dict)

if __name__ == '__main__':

	if len(sys.argv) != 3:

		#print("Invalid input format")
		#print("python [filename] [input file] [round_robin number X]")
		sys.exit()

	#print(str(sys.argv[1]))
	#print(int(sys.argv[2]))

	# filename = "input.txt"
	filename = str(sys.argv[1])
	# round_robin = 7
	round_robin = int(sys.argv[2])


	ouput_file = open(output_filename,"w")
	ouput_file.close()

	input_file = open(filename,"r")
	line = input_file.readline().strip()
	
	data_elements_on_disk,data_elements_in_memory = create_data_dictionary(line.strip())

	# #print(data_elements_on_disk)
	# #print(data_elements_in_memory)

	count = 1

	transaction_dict = {}
	new_transaction = False

	while line:
		
		line = input_file.readline()
		
		if new_transaction == True and len(line.strip())> 0 and line[0] == "T":
			#print()
			transaction_name, num_lines = line.split()
			num_lines = int(num_lines)
			transaction_dict[transaction_name] = []

			for i in range(num_lines):
				line = input_file.readline().strip()
				transaction_dict[transaction_name].append(line)				
				
				#print(line)

			new_transaction == False

		elif len(line.strip()) == 0:
			
			new_transaction = True

	# #print(transaction_dict)
	input_file.close()

	# #print(len(transaction_dict))
	#print()
	#print()
	generate_undo_log(round_robin,data_elements_on_disk,data_elements_in_memory,transaction_dict)
					