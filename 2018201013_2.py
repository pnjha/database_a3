import copy
import os
import sys
import re


output_filename = "2018201013_2.txt"

def create_data_dictionary(file_line):

	element_list = file_line.split(" ")

	data_dict = {}

	for i in range(0,len(element_list),2):

		data_dict[element_list[i]] = int(element_list[i+1])

	return data_dict

def perform_recovery(data_elements_dict,log_records_list):

	seen_end_ckpt = False

	transaction_dict = {}

	for record in reversed(log_records_list):
		
		if "END CKPT" in record:
			seen_end_ckpt = True
		elif "START CKPT" in record:
			if seen_end_ckpt == True:
				break
		elif "COMMIT" in record or "START" in record:
			status,transaction_id = re.split("<|\s|>",record)[1:-1]
			
			status = status.strip()
			transaction_id = transaction_id.strip()

			transaction_dict[transaction_id] = "done"
		else:
			transaction_id, data_element, value = re.split("<|,|>",record)[1:-1]

			data_element = data_element.strip()
			transaction_id = transaction_id.strip()
			value = int(value.strip())

			if transaction_id not in transaction_dict:
				transaction_dict[transaction_id] = "not done"				

			if transaction_dict[transaction_id] != "done":
				data_elements_dict[data_element] = value

	output_flie = open(output_filename,"w")
	log_record = ""

	for item in sorted (data_elements_dict.keys()):
		if data_elements_dict[item] != None:
			log_record += item + " " + str(data_elements_dict[item]) + " "

	log_record = log_record.strip()

	output_flie.write(log_record)
	output_flie.close()


if __name__ == '__main__':

	if len(sys.argv) != 2:

		print("Invalid input format")
		print("python [filename] [input file]")
		sys.exit()

	
	filename = str(sys.argv[1])

	input_file = open(filename,"r")
	line = input_file.readline().strip()
	
	data_elements = create_data_dictionary(line)

	# print(data_elements)

	flag = True

	log_records_list = []

	while line:
		
		line = input_file.readline().strip()
		# print(line)

		if len(line) > 0:
			log_records_list.append(line.strip())


		if len(line) == 0 and flag == True:
			line = " "
			flag = False

	input_file.close()

	perform_recovery(data_elements,log_records_list)


