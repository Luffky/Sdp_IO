from pyspark import SparkContext, SparkConf, RDD
import numpy as np
import math
from collections import defaultdict
import hashlib
global bytebuffer

bytebuffer = []

def SDPPartitioner_pharp_alluxio(key):
	'''
		Partitioner_function
	'''
	return int(str(key).split(',')[2])

def SDPPartitioner(key):
	'''
		Partitioner_function
	'''
	return int(str(key).split(',')[4])

def extract_lsm_handle():
	initset = []
	beam = 0
	major_loop = 0
	initset.append((beam, major_loop))
	return sc.parallelize(initset).map(extract_lsm_kernel)

def local_sky_model_handle():
	initset = []
	initset.append(())
	return sc.parallelize(initset).map(local_sky_model_kernel)

def telescope_management_handle():
	initset = []
	initset.append(())
	return sc.parallelize(initset).map(telescope_management_kernel)

def visibility_buffer_handle():
	initset = []
	beam = 0
	for frequency in range(0, 20):
		time = 0 
		baseline = 0
		polarisation = 0
		initset.append((beam, frequency, time, baseline, polarisation))
	return sc.parallelize(initset).map(visibility_buffer_kernel)

def telescope_data_handle(telescope_management):
	dep_telescope_management = defaultdict(list)
	beam = 0
	frequency = 0
	time = 0
	baseline = 0
	dep_telescope_management[()] = [(beam, frequency, time, baseline)]
	input_telescope_management = telescope_management.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]),dep_telescope_management[ix_data[0]]))
	return input_telescope_management.groupByKey().mapValues(lambda x: (x)).map(telescope_data_kernel)

def reppre_ifft_handle(broads_input_telescope_data, broadcast_lsm):
	initset = []
	dep_extract_lsm = defaultdict(list)
	beam = 0
	major_loop = 0
	for frequency in range(0, 5):
		time = 0
		for facet in range(0, 49):
			for polarisation in range(0, 4):
				# dep_extract_lsm[(beam, major_loop)] = [(beam, major_loop, frequency, time, facet, polarisation)]
				initset.append((beam, major_loop, frequency, time, facet, polarisation))

	return sc.parallelize(initset).map(lambda ix: reppre_ifft_kernel((ix, broads_input_telescope_data, broadcast_lsm)))

def degrid_handle(reppre_ifft, broads_input_telescope_data, broadcast_lsm):
	return reppre_ifft.flatMap(lambda ix: degrid_kernel((ix, broads_input_telescope_data, broadcast_lsm)))

def pharotpre_dft_sumvis_handle(degrid):
	dep_extract_lsm = defaultdict(list)
	dep_degkerupd_deg = defaultdict(list)
	initset = []
	beam = 0
	for frequency in range(0, 20):
		time = 0
		baseline = 0
		polarisation = 0
		initset.append((beam, frequency, time, baseline, polarisation))
	return degrid.partitionBy(20, SDPPartitioner_pharp_alluxio).mapPartitions(pharotpre_dft_sumvis_kernel)

def timeslots_handle():
	initset = []
	beam = 0
	for time in range(0, 8):
		frequency = 0
		baseline = 0
		polarisation = 0
		major_loop = 0
		initset.append((beam, major_loop, frequency, time, baseline, polarisation))

	return sc.parallelize(initset).map(timeslots_kernel)

def solve_handle(timeslots):
	dep_timeslots = defaultdict(list)
	beam = 0
	major_loop = 0
	frequency = 0
	for time in range(0, 8):
		polarisation = 0
		dep_timeslots[(beam, major_loop, frequency, time, 0, polarisation)] = [(beam, major_loop, frequency, time, polarisation)]
	input_timeslots = timeslots.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_timeslots[ix_data[0]]))
	return input_timeslots.groupByKey().mapValues(lambda x: (x)).map(solve_kernel)

def cor_subvis_flag_handle():
	initset = []
	beam = 0
	for frequency in range(0, 20):
		time = 0 
		baseline = 0
		polarisation = 0
		major_loop = 0
		initset.append((beam, major_loop, frequency, time, baseline, polarisation))
	return sc.parallelize(initset).map(cor_subvis_flag_kernel)

def grikerupd_pharot_grid_fft_rep_handle(broads_input_telescope_data, broads_input1):
	initset = []
	beam = 0
	frequency = 0
	for facet in range(0, 49):
		for polarisation in range(0, 4):
			time = 0
			major_loop = 0
			initset.append((beam, major_loop, frequency, time, facet, polarisation))
	return sc.parallelize(initset).map(lambda ix: grikerupd_pharot_grid_fft_rep_kernel((ix, broads_input_telescope_data, broads_input1)))

def sum_facets_handle(grikerupd_pharot_grid_fft_rep):
	initset = []
	beam = 0
	frequency = 0
	for facet in range(0, 49):
		for polarisation in range(0, 4):
			time = 0
			major_loop = 0
			initset.append((beam, major_loop, frequency, time, facet, polarisation))
	return grikerupd_pharot_grid_fft_rep.map(sum_facets_kernel)

def identify_component_handle(sum_facets):
	dep_sum_facets = defaultdict(list)
	beam = 0
	major_loop = 0
	frequency = 0
	for facet in range(0, 49):
		for i_polarisation in range(0, 4):
			dep_sum_facets[(beam, major_loop, frequency, 0, facet, i_polarisation)] = [(beam, major_loop, frequency, facet)]
	return sum_facets.partitionBy(49, SDPPartitioner).mapPartitions(identify_component_kernel_partitions)

def source_find_handle(identify_component):
	dep_identify_component = defaultdict(list)
	beam = 0
	major_loop = 0
	for i_facet in range(0, 49):
		dep_identify_component[(beam, major_loop, 0, i_facet)] = [(beam, major_loop)]
	input_identify_component = identify_component.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_identify_component[ix_data[0]]))
	return input_identify_component.groupByKey().mapValues(lambda x: (x)).map(source_find_kernel)

def subimacom_handle(sum_facets, broads_input2):
	dep_identify_component = defaultdict(list)
	dep_sum_facets = defaultdict(list)
	beam = 0
	major_loop = 0
	frequency = 0
	for facet in range(0, 49):
		for polarisation in range(0, 4):
			dep_identify_component[(beam, major_loop, frequency, facet)] = [(beam, major_loop, frequency, facet, polarisation)]
			dep_sum_facets[(beam, major_loop, frequency, 0, facet, polarisation)] = [(beam, major_loop, frequency, facet, polarisation)]
	return sum_facets.map(lambda ix: subimacom_kernel((ix, broads_input2)))
	
def update_lsm_handle(local_sky_model, source_find):
	dep_local_sky_model = defaultdict(list)
	dep_source_find = defaultdict(list)
	beam = 0
	major_loop = 0
	dep_local_sky_model[()] = [(beam, major_loop)]
	dep_source_find[(beam, major_loop)] = [(beam, major_loop)]
	input_local_sky_model = local_sky_model.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_local_sky_model[ix_data[0]]))
	input_source_find = source_find.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_source_find[ix_data[0]]))
	return input_local_sky_model.cogroup(input_source_find).map(update_lsm_kernel)



def extract_lsm_kernel(ix):
	label = "Extract_LSM (0.0 MB, 0.00 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = bytearray(max(4, int(scale_data * 0)))
	#ByteBuffer.wrap(result).putInt(0, hash)
	return (ix, result)

def local_sky_model_kernel(ix):
	label = "Local Sky Model (0.0 MB, 0.00 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = bytearray(max(4, int(scale_data * 0)))
	bytebuffer.append(result)
	#ByteBuffer.wrap(result).putInt(0, hash)
	return (ix, result)

def telescope_management_kernel(ix):
	label = "Telescope Mnagement (0.0 MB, 0.00 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = bytearray(max(4, int(scale_data * 0)))
	#ByteBuffer.wrap(result).putInt(0, hash)
	return (ix, result)

def using(func, result):
	try:
		return func(result)
	finally:
		print "there is something wrong"

def visibility_buffer_kernel(ix):
	label = "Visibility Buffer (46168.5 MB, 0.00 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + "MB input)")
	# Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioHost)
	# Configuration.set(PropertyKey.MASTER_PRC_PORT, alluxioPort)	
	# System.setProperty("HADDOOP_USER_NAME", "hadoop")
	# fs = FileSystem.Factory.get()
	# path = AlluxioURI("/visibility_buffer/" + ix[1])
	result = bytearray(max(4, int(scale_data * 153895035)))
	# using(fs.createFile(path, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)))
	return (ix, )

def telescope_data_kernel(x):
	ix, data_telescope_management = x
	label = "Telescope Data (0.0 MB, 0.00 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	for data in data_telescope_management:
		Hash ^= hash(bytes(data[0:4]))
		input_size += len(data)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = bytearray(max(4, int(scale_data * 0)))
	#ByteBuffer.wrap(result).putInt(0, hash) 
	return (ix, result)

def reppre_ifft_kernel(ix):
	label = "Reprojection Predict + IFFT (24209.8 MB, 0.39 Tflop) " + str(ix)
	result = bytearray(max(4, int(scale_data * 80699223)))
	# Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioHost)
	# Configuration.set(PropertyKey.MASTER_PRC_PORT, alluxioPort)
	# System.setProperty("HADDOOP_USER_NAME", "hadoop")
	# fs = FileSystem.Factory.get()
	# path = AlluxioURI("/reppre_ifft/" + ix[0][0] + "_" + ix[0][1] + "_" + ix[0][2] + "_" + ix[0][3] + "_" + ix[0][4] + "_" + ix[0][5])
	# using(fs.createFile(path, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)))
	return ix[0]

def degrid_kernel(ix):
	# Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioHost)
	# Configuration.set(PropertyKey.MASTER_PRC_PORT, alluxioPort)
	# System.setProperty("HADDOOP_USER_NAME", "hadoop")
	# fs = FileSystem.Factory.get()
	# temp = ix[0][0] + "_" + ix[0][1] + "_" + ix[0][2] + "_" + ix[0][3] + "_" + ix[0][4] + "_" + ix[0][5]
	# path = AlluxioURI("/reppre_ifft/" + temp)
	# using(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE)))
	mylist = np.empty(4, list)
	result2 = bytearray(max(4, int(scale_data * 998572)))
	result2_2 = bytearray(max(4, int(scale_data * 998572)))
	result2_3 = bytearray(max(4, int(scale_data * 998572)))
	result2_4 = bytearray(max(4, int(scale_data * 998572)))

	temp1 = ix[0][2] * 4
	mylist[0] = ((ix[0][0], ix[0][1], temp1, ix[0][3], ix[0][4], ix[0][5]), result2)
	temp2 = ix[0][2] * 4 + 1
	mylist[1] = ((ix[0][0], ix[0][1], temp2, ix[0][3], ix[0][4], ix[0][5]), result2)
	temp3 = ix[0][2] * 4 + 2
	mylist[2] = ((ix[0][0], ix[0][1], temp3, ix[0][3], ix[0][4], ix[0][5]), result2)
	temp4 = ix[0][2] * 4 + 3
	mylist[3] = ((ix[0][0], ix[0][1], temp4, ix[0][3], ix[0][4], ix[0][5]), result2)

	return mylist 

def degkerupd_deg_kernel():
	pass

def pharotpre_dft_sumvis_kernel(ix):
	label = "Phase Rotation Predict + DFT + Sum visibilities (36384.6 MB, 102.32 Tflop) " + str(ix)
	# Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioHost)
	# Configuration.set(PropertyKey.MASTER_PRC_PORT, alluxioPort)	
	# System.setProperty("HADDOOP_USER_NAME", "hadoop")
	# fs = FileSystem.Factory.get()
	if ix:
		temp = ix.next() 
		result2 = np.empty(1, list)
		result2[0] = (temp[0][0], temp[0][1], temp[0][2], temp[0][3], temp[0][5])

		result = bytearray(max(4, int(scale_data * 363846353)))
		
		# path3 = AlluxioURI("/pharotpre_dft_sumvis/" + temp[0][2])
		# using(fs.createFile(path3, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)))

		return iter(result2)
	else:
		result2 = np.empty(1, list)
		result2[0] = ((0, 0, 0, 0, 0))
		return iter(result2)

def timeslots_kernel(ix):
	# Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioHost)
	# Configuration.set(PropertyKey.MASTER_PRC_PORT, alluxioPort)
	# System.setProperty("HADDOOP_USER_NAME", "hadoop")
	# fs = FileSystem.Factory.get()
	for frequency in range(0, 20):
		temp = frequency
		# path = AlluxioURI("/pharotpre_dft_sumvis/" + temp)
		# path2 = AlluxioURI("/visibility_buffer/" + temp)
		# using(fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE)))
		# using(fs.openFile(path2, OpenFileOptions.defaults().setReadType(ReadType.CACHE)))
	result = bytearray(max(4, int(scale_data * 15182856)))
	return (ix, result)

def solve_kernel(x):
	ix, data_timeslots = x
	label = "Solve (8262,8 MB, 2939.73 Tflop) " + str(ix)
	Hash = hash(label)
	# Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioHost)
	# Configuration.set(PropertyKey.MASTER_PRC_PORT, alluxioPort)
	# System.setProperty("HADDOOP_USER_NAME", "hadoop")
	# fs = FileSystem.Factory.get()
	result = bytearray(max(4, int(scale_data * 27542596)))
	# path3 = AlluxioURI("/solve/" + str(ix[3]))
	# using(fs.createFile(path3, CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)))
	return ix

def cor_subvis_flag_kernel(ix):
	label = "Correct + subtract Visibility + Flag (46639,6 MB, 1.24 Tflop) " + str(ix)
	# temp = ix[2]
	# Configuration.set(PropertyKey.MASTER_HOSTNAME, alluxioHost)
	# Configuration.set(PropertyKey.MASTER_PRC_PORT, alluxioPort)
	# System.setProperty("HADDOOP_USER_NAME", "hadoop")
	# fs = FileSystem.Factory.get()
	# path = AlluxioURI("/pharotpre_dft_sumvis/" + temp)
	# path2 = AlluxioURI("/visibility_buffer/" + temp)
	# in1 = fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))
	# buf = bytearray(int(in1.remaining()))
	# tempdata = in1.read(buf)
	# in2 = fs.openFile(path, OpenFileOptions.defaults().setReadType(ReadType.CACHE))
	# buf2 = bytearray(int(in2.remaining()))
	# tempdata2 = in2.read(buf2)
	# for time in range(0, 6):
	# 	pathTime = AlluxioURI("/solve/" + time)
	# 	n11 = fs.
	# 	buf3
	# 	tempdata3
	result = bytearray(max(4, int(scale_data * 155465392)))
	return (ix, result)

def grikerupd_pharot_grid_fft_rep_kernel(ix):
	label = "Gridding Kernel Update + Phase Rotation + Grid + FFT + Reprojection (24208.9MB, 2.34 Tflop) " + str(ix)
	result = bytearray(max(4, int(scale_data * 80696289)))
	return (ix[0], result)

def sum_facets_kernel(ix):
	label = "Sum Facets (24208.9 MB, 0.00 Tflop) " + str(ix)
	result = bytearray(max(4, int(scale_data * 80696289)))
	return (ix[0], result)

def identify_component_kernel_partitions(ix):
	result = bytearray(max(4, int(scale_data * 1600)))
	result2 = np.empty(1, list)
	if ix:
		temp = ix.next()
		result2[0] = ((temp[0][0], temp[0][1], temp[0][2], temp[0][4]), result)
		return iter(result2)

	else:
		result2[0] = ((0, 0, 0, 0), result)
		return iter(result2)

def source_find_kernel(x):
	ix, data_identify_component = x
	label = "Source Find (5.8 MB, 0.00 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	for data in data_identify_component:
		Hash ^= hash(bytes(data[0:4]))
		input_size += len(data)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = bytearray(max(4, int(scale_data * 57600)))
	# Bytebuffer.wrap(result).putInt(0. hash)
	return (ix, result)

def subimacom_kernel(ix):
	label = "Subtract Image Component (167.9 MB, 4118.87 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = bytearray(max(4, int(scale_data * 1678540)))
	#ByteBuffer.wrap(result).putInt(0, Hash)
	aa = ix[0][0]
	return ((aa[0], aa[1], aa[2], aa[4], aa[5]), result)

def update_lsm_kernel(x):
	ix, (data_local_sky_model, data_source_find) = x
	label = "Update LSM (0.0 MB, 0.00 Tflop) " + str(ix)
	Hash = hash(label)
	input_size = 0
	for data in data_local_sky_model:
		Hash ^= hash(bytes(data[0:4]))
		input_size += len(data)
	for data in data_source_find:
		Hash ^= hash(bytes(data[0:4]))
		input_size += len(data)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = bytearray(max(4, int(scale_data * 0)))
	#ByteBuffer.wrap(result).putInt(0, Hash)
	return (ix, result)



#bytearray replace Array[Byte]
scale_data = 0.0000000001
scale_compute = 1.0
alluxioHost = "hadoop8"
alluxioPort = "19998"

if __name__ == '__main__':
	conf = SparkConf().setAppName("SDP Pipeline").setMaster("local[1]")
	sc = SparkContext(conf=conf)
	# === Extract Lsm ===
	extract_lsm = extract_lsm_handle()
	# === Local Sky Model ===
	local_sky_model = local_sky_model_handle()
	# === Telescope Management ===
	telescope_management = telescope_management_handle()
	# === Visibility Buffer ===
	visibility_buffer = visibility_buffer_handle()
	print(" the size of new rdd visibility_buffer " + str(visibility_buffer.count()))
	# === Telescope Data ===
	telescope_data = telescope_data_handle(telescope_management)

	broads_input_telescope_data = sc.broadcast(telescope_data.collect())
	broadcast_lsm = sc.broadcast((extract_lsm.collect()))
	# === reppre_ifft ===
	reppre_ifft = reppre_ifft_handle(broads_input_telescope_data, broadcast_lsm)
	# === degrid ===
	degrid = degrid_handle(reppre_ifft, broads_input_telescope_data, broadcast_lsm)
	degrid.cache()
	# === pharotpre_dft_sumvis ===
	pharotpre_dft_sumvis = pharotpre_dft_sumvis_handle(degrid)
	print(" the size of new rdd pharotpre_dft_sumvis " + str(pharotpre_dft_sumvis.count()))
	# === Timeslots ===
	timeslots = timeslots_handle()
	# === solve ===
	solve = solve_handle(timeslots)
	print("solve count " + str(solve.count()))
	# === correct + Subtract Visibility + Flag ===
	cor_subvis_flag = cor_subvis_flag_handle()
	cor_subvis_flag.cache()

	broads_input1 = sc.broadcast(cor_subvis_flag.collect())
	# === Gridding Kernel Update + Phase Rotation + Grid + FFT + Rreprojection ===
	grikerupd_pharot_grid_fft_rep = grikerupd_pharot_grid_fft_rep_handle(broads_input_telescope_data, broads_input1)
	print("the size of new rdd grikerupd_pharot_grid_fft_rep " + str(grikerupd_pharot_grid_fft_rep.count()))
	# ===Sum Facets ===
	sum_facets = sum_facets_handle(grikerupd_pharot_grid_fft_rep)
	print("the size of new rdd sum_facets " + str(sum_facets.count()))
	sum_facets.cache()
	# === Identify Component ===
	identify_component = identify_component_handle(sum_facets)
	broads_input2 = sc.broadcast(identify_component.collect())
	# === Source Find ===
	source_find = source_find_handle(identify_component)
	print("the size of new rdd source_find " + str(source_find.count()))
	# === Substract Image Component ===
	subimacom = subimacom_handle(sum_facets, broads_input2)
	# === Update LSM ===
	update_lsm = update_lsm_handle(local_sky_model, source_find)	
	# === Terminate ===
	print("Finishing...")
	print("Update LSM: %d" % update_lsm.count())
	print("Subtract Image Component: %d" % subimacom.count())