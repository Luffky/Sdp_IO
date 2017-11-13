from pyspark import SparkContext, SparkConf, RDD
import numpy as np
import math
from collections import defaultdict
import hashlib
import functools
 



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

def MapPartitioner(partitions):
	def _inter(key):
		partition = partitions
		return partition[key]
	return _inter

def hash(st):
	temp = hashlib.md5()
	temp.update(st)
	t = int(temp.hexdigest()[0:7], 16)
	return t

def extract_lsm_handle():
	partitions = defaultdict(int)
	partition = 0
	initset = []
	beam = 0
	major_loop = 0
	partitions[(beam, major_loop)] = partition
	partition += 1
	initset.append(((beam, major_loop), ()))
	partitioner = MapPartitioner(partitions)
	return sc.parallelize(initset).partitionBy(len(partitions), partitioner).mapPartitions(extract_lsm_kernel, True)

def local_sky_model_handle():
	partitions = defaultdict(int)
	partition = 0
	initset = []
	partitions[()] = partition
	partition += 1
	initset.append(((), ()))
	partitioner = MapPartitioner(partitions)
	return sc.parallelize(initset).partitionBy(len(partitions), partitioner).mapPartitions(local_sky_model_kernel, True)

def telescope_management_handle():
	partitions = defaultdict(int)
	partition = 0
	initset = []
	partitions[()] = partition
	partition += 1
	initset.append(((), ()))
	partitioner = MapPartitioner(partitions)
	return sc.parallelize(initset).partitionBy(len(partitions), partitioner).mapPartitions(telescope_management_kernel, True)

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
	partitions = defaultdict(int)
	partition = 0
	dep_telescope_management = defaultdict(list)
	beam = 0
	frequency = 0
	time = 0
	baseline = 0
	partitions[(beam, frequency, time, baseline)] = partition
	partition += 1
	dep_telescope_management[()] = [(beam, frequency, time, baseline)]
	input_telescope_management = telescope_management.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]),dep_telescope_management[ix_data[0]]))
	partitioner = MapPartitioner(partitions)
	return input_telescope_management.partitionBy(len(partitions), partitioner).mapPartitions(telescope_data_kernel, True)

def reppre_ifft_handle(broadcast_lsm):
	initset = []
	dep_extract_lsm = defaultdict(list)
	beam = 0
	major_loop = 0
	for frequency in range(0, 5):
		time = 0
		for facet in range(0, 81):
			for polarisation in range(0, 4):
				# dep_extract_lsm[(beam, major_loop)] = [(beam, major_loop, frequency, time, facet, polarisation)]
				initset.append((beam, major_loop, frequency, time, facet, polarisation))

	return sc.parallelize(initset).map(lambda ix: reppre_ifft_kernel((ix, broadcast_lsm)))

def degrid_handle(reppre_ifft, broads_input_telescope_data):
	return reppre_ifft.flatMap(lambda ix: degrid_kernel((ix, broads_input_telescope_data)))

def pharotpre_dft_sumvis_handle(degrid, broadcast_lsm):
	dep_extract_lsm = defaultdict(list)
	dep_degkerupd_deg = defaultdict(list)
	initset = []
	beam = 0
	for frequency in range(0, 20):
		time = 0
		baseline = 0
		polarisation = 0
		initset.append((beam, frequency, time, baseline, polarisation))
	return degrid.partitionBy(20, SDPPartitioner_pharp_alluxio).mapPartitions(lambda ix: pharotpre_dft_sumvis_kernel((ix, broadcast_lsm)))

def timeslots_handle(broads_input0, broads_input1):
	initset = []
	beam = 0
	for time in range(0, 120):
		frequency = 0
		baseline = 0
		polarisation = 0
		major_loop = 0
		initset.append((beam, major_loop, frequency, time, baseline, polarisation))

	return sc.parallelize(initset, 24).map(lambda ix: timeslots_kernel((ix, broads_input0, broads_input1)))

def solve_handle(timeslots):
	dep_timeslots = defaultdict(list)
	beam = 0
	major_loop = 0
	baseline = 0
	frequency = 0
	for time in range(0, 120):
		polarisation = 0
		dep_timeslots[(beam, major_loop, frequency, time, baseline, polarisation)] = (beam, major_loop, frequency, time, baseline, polarisation)
	return timeslots.map(solve_kernel)

def cor_subvis_flag_handle(broads_input0, broads_input1, broads_input2):
	initset = []
	beam = 0
	for frequency in range(0, 20):
		time = 0 
		baseline = 0
		polarisation = 0
		major_loop = 0
		initset.append((beam, major_loop, frequency, time, baseline, polarisation))
	return sc.parallelize(initset, 20).map(lambda ix: cor_subvis_flag_kernel((ix, broads_input0, broads_input1, broads_input2)))

def grikerupd_pharot_grid_fft_rep_handle(broads_input_telescope_data, broads_input):
	initset = []
	beam = 0
	frequency = 0
	for facet in range(0, 81):
		for polarisation in range(0, 4):
			time = 0
			major_loop = 0
			initset.append((beam, major_loop, frequency, time, facet, polarisation))
	return sc.parallelize(initset).map(lambda ix: grikerupd_pharot_grid_fft_rep_kernel((ix, broads_input_telescope_data, broads_input)))

def sum_facets_handle(grikerupd_pharot_grid_fft_rep):
	initset = []
	beam = 0
	frequency = 0
	for facet in range(0, 81):
		for polarisation in range(0, 4):
			time = 0
			major_loop = 0
			initset.append((beam, major_loop, frequency, time, facet, polarisation))
	return grikerupd_pharot_grid_fft_rep.map(sum_facets_kernel)

def identify_component_handle(sum_facets):
	partitions = defaultdict(int)
	partition = 0
	dep_sum_facets = defaultdict(list)
	beam = 0
	major_loop = 0
	frequency = 0
	for facet in range(0, 81):
		partitions[(beam, major_loop, frequency, facet)] = partition
		partition += 1
		for i_polarisation in range(0, 4):
			dep_sum_facets[(beam, major_loop, frequency, 0, facet, i_polarisation)] = [(beam, major_loop, frequency, facet)]
	return sum_facets.partitionBy(81, SDPPartitioner).mapPartitions(identify_component_kernel_partitions)

def source_find_handle(identify_component):
   	partitions = defaultdict(int)
	partition = 0
	dep_identify_component = defaultdict(list)
	beam = 0
	major_loop = 0
	partitions[(beam, major_loop)] = partition
	partition += 1
	for i_facet in range(0, 81):
		dep_identify_component[(beam, major_loop, 0, i_facet)] = [(beam, major_loop)]
	input_identify_component = identify_component.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_identify_component[ix_data[0]]))
	partitioner = MapPartitioner(partitions)
	return input_identify_component.partitionBy(len(partitions), partitioner).mapPartitions(source_find_kernel, True)

def subimacom_handle(sum_facets, identify_component):
	partitions = defaultdict(int)
	partition = 0
	dep_identify_component = defaultdict(list)
	dep_sum_facets = defaultdict(list)
	beam = 0
	major_loop = 0
	frequency = 0
	for facet in range(0, 81):
		partitions[(beam, major_loop, frequency, facet)] = partition
		partition += 1
		for polarisation in range(0, 4):
			dep_sum_facets[(beam, major_loop, frequency, 0, facet, polarisation)] = (beam, major_loop, frequency, facet)
		dep_identify_component[(beam, major_loop, frequency, facet)] = (beam, major_loop, frequency, facet)
	input_identify_component = identify_component.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_identify_component[ix_data[0]]))
	input_sum_facets = identify_component.flatMap(lambda ix_data: map(lambda  x: (x, ix_data[1]), dep_sum_facets[ix_data[0]]))
	partitioner = MapPartitioner(partitions)
	return input_identify_component.partitionBy(len(partitions), partitioner).cogroup(input_sum_facets.partitionBy(len(partitions), partitioner)).mapPartitions(subimacom_kernel)

def update_lsm_handle(local_sky_model, source_find):
	partitions = defaultdict(int)
	partition = 0
	dep_local_sky_model = defaultdict(list)
	dep_source_find = defaultdict(list)
	beam = 0
	major_loop = 0
	partitions[(beam, major_loop)] = partition
	partition += 1
	dep_local_sky_model[()] = [(beam, major_loop)]
	dep_source_find[(beam, major_loop)] = [(beam, major_loop)]
	input_local_sky_model = local_sky_model.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_local_sky_model[ix_data[0]]))
	input_source_find = source_find.flatMap(lambda ix_data: map(lambda x: (x, ix_data[1]), dep_source_find[ix_data[0]]))
	partitioner = MapPartitioner(partitions)
	# print 100*'-'
	# print input_source_find.cache().collect()
	# print input_local_sky_model.cache().collect()
	# print input_local_sky_model.partitionBy(len(partitions), partitioner).cogroup(input_source_find.partitionBy(len(partitions), partitioner)).collect()
	# print 100*'-'
	return input_local_sky_model.partitionBy(len(partitions), partitioner).cogroup(input_source_find.partitionBy(len(partitions), partitioner)).mapPartitions(update_lsm_kernel, True)







def extract_lsm_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = ixs.next()[0]
	label = "Extract_LSM (0.0 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + "(Hash " + hex(Hash) +  " from " + str((input_size / 1000000)) + " MB input")
 	result = np.zeros(max(1, int(scale_data * 0)), int)
 	result[0] = Hash
 	return iter([(ix, result)])

def local_sky_model_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = ixs.next()[0]
	label = "Local Sky Model (0.0 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (Hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 0)), int)
	result[0] = Hash
	return iter([(ix, result)])

def telescope_management_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = ixs.next()[0]
	label = "Telescope Management (0.0 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 0)), int)
	result[0] = Hash
	return iter([(ix, result)])


def visibility_buffer_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = ixs
	label = "Visibility Buffer (546937.1 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + "MB input)")
	result = np.zeros(max(1, int(scale_data * 1823123744)), int)
	result[0] = Hash
	return (ix, result)

def telescope_data_kernel(ixs):
	data_telescope_management = ixs
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0)
	for dix, data in data_telescope_management:
		Hash ^= data[0]
		input_size += data.shape[0]
		ix = dix

	label = "Telescope Data (0.0 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 0)), int)
	result[0] = Hash
	return iter([(ix, result)])

def reppre_ifft_kernel(ixs):
	reppre, data_extract_lsm = ixs
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	ix = reppre
	for dix, data in data_extract_lsm.value:
		Hash ^= data[0]
		input_size += data.shape[0]
	label = "Reprojection Predict + IFFT (14645.6 MB, 2.56 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input")
	result = np.zeros(max(1, int(scale_data * 48818555)), int)
	result[0] = Hash
	return (ix, result)

def degrid_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	label = "Degridding Kernel Update + Degrid (674.8 MB, 0.59 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	mylist = np.empty(4, list)
	result1 = np.zeros(max(4, int(scale_data * 2249494)), int)
	result1[0] = Hash
	result2 = np.zeros(max(4, int(scale_data * 2249494)), int)
	result2[0] = Hash
	result3 = np.zeros(max(4, int(scale_data * 2249494)), int)
	result3[0] = Hash
	result4 = np.zeros(max(4, int(scale_data * 2249494)), int)
	result4[0] = Hash

	temp1 = ix[2] * 4
	mylist[0] = ((ix[0], ix[1], temp1, ix[3], ix[4], ix[5]), result1)
	temp2 = ix[2] * 4 + 1
	mylist[1] = ((ix[0], ix[1], temp2, ix[3], ix[4], ix[5]), result2)
	temp3 = ix[2] * 4 + 2
	mylist[2] = ((ix[0], ix[1], temp3, ix[3], ix[4], ix[5]), result3)
	temp4 = ix[2] * 4 + 3
	mylist[3] = ((ix[0], ix[1], temp4, ix[3], ix[4], ix[5]), result4)

	return mylist 


def pharotpre_dft_sumvis_kernel(ixs):
	data_degkerupd_deg, data_extract_lsm = ixs
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	label = "Phase Rotation Predict + DFT + Sum visibilities (546937.1 MB, 512.53 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 1823123744)), int)
	result[0] = Hash
	return iter([(ix, result)])

def timeslots_kernel(ixs):
	idx, data_visibility_buffer, data_pharotpre_dft_sumvis = ixs
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	ix = idx
	label = "Timeslots (1518.3 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 5060952)), int)
	result[0] = Hash

	return (ix, result)

def solve_kernel(ixs):
	dix, data = ixs
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	Hash ^= data[0]
	input_size += data.shape[0]
	ix = dix
	label = "Solve (8262.8 MB, 16.63 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 27542596)), int)
	result[0] = Hash
	return (ix, result)

def cor_subvis_flag_kernel(ixs):
	ix, data_pharotpre_dft_sumvis, data_visibility_buffer, data_solve = ixs
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	label = "Correct + Subtract Visibility + Flag (153534.1 MB, 4.08 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 511780275)), int)
	result[0] = Hash
	return (ix, result)

def grikerupd_pharot_grid_fft_rep_kernel(ixs):
	idx, data_telescope_data, data_cor_subvis_flag = ixs
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	ix = idx
	label = "Gridding Kernel Update + Phase Rotation + Grid + FFT + Reprojection (14644.9 MB, 20.06 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 48816273)), int)
	result[0] = Hash
	return (ix, result)

def sum_facets_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	dix, data = ixs
	Hash ^= data[0]
	input_size += data.shape[0]
	ix = dix

	label = "Sum Facets (14644.9 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 48816273)), int)
	result[0] = Hash
	return (ix, result)

def identify_component_kernel_partitions(ixs):
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0, 0, 0)
	for dix, data in ixs:
		Hash ^= data[0]
		input_size += data.shape[0]
		ix = dix
	label = "Identify Component (0.2 MB, 1830.61 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 533)), int)
	result[0] = Hash
	return iter([((ix[0], ix[1], ix[2], ix[4]), result)])

def source_find_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = (0, 0)
	for dix, data in ixs:
		Hash ^= data[0]
		input_size += data.shape[0]
		ix = dix
	label = "Source Find (5.8 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 19200)), int)
	result[0] = Hash
	return iter([(ix, result)])

def subimacom_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = (0, 0, 0, 0)
	# for temp in ixs:
	# 	ix, (data_identify_component, data_sum_facets) = temp
	# 	for data in data_identify_component:
	# 		Hash ^= data[0]
	# 		input_size += 1
	# 	for data in data_sum_facets:
	# 		Hash ^= data[0]
	# 		input_size += 1

	label = "Subtract Image Component (73224.4 MB, 67.14 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 244081369)), int)
	result[0] = Hash
	return iter([(ix, result)])

def update_lsm_kernel(ixs):
	Hash = 0
	input_size = 0
	ix = (0, 0)
	for temp in ixs:

		ix, (data_local_sky_mode, data_source_find) = temp
		for data in data_local_sky_mode:
			Hash ^= data[0]
			input_size += 1

		for data in data_source_find:
			Hash ^= data[0]
			input_size += 1

	label = "Update LSM (0.0 MB, 0.00 Tflop) " + str(ix).replace(" ", "")
	Hash ^= hash(label)
	print(label + " (hash " + hex(Hash) + " from " + str(input_size / 1000000) + " MB input)")
	result = np.zeros(max(1, int(scale_data * 0)), int)
	result[0] = Hash
	return iter([(ix, result)])





scale_data = 0.000001
scale_compute = 0


if __name__ == '__main__':
	conf = SparkConf().setAppName("SDP Pipeline").setMaster("local[1]")
	sc = SparkContext(conf=conf)

	# === Extract Lsm ===
	extract_lsm = extract_lsm_handle()
	broadcast_lsm = sc.broadcast(extract_lsm.collect())
	# === Local Sky Model ===	
	local_sky_model = local_sky_model_handle()
	# === Telescope Management ===
	telescope_management = telescope_management_handle()
	# === Visibility Buffer ===
	visibility_buffer = visibility_buffer_handle()
	visibility_buffer.cache()
	broads_input1 = sc.broadcast(visibility_buffer.collect())
	# === reppre_ifft ===
	reppre_ifft = reppre_ifft_handle(broadcast_lsm)
	reppre_ifft.cache()
	# === Telescope Data ===
	telescope_data = telescope_data_handle(telescope_management)
	broads_input_telescope_data = sc.broadcast(telescope_data.collect())
	# === degrid ===
	degrid = degrid_handle(reppre_ifft, broads_input_telescope_data)
	degrid.cache()
	# === pharotpre_dft_sumvis ===
	pharotpre_dft_sumvis = pharotpre_dft_sumvis_handle(degrid, broadcast_lsm)
	pharotpre_dft_sumvis.cache()
	broads_input0 = sc.broadcast(pharotpre_dft_sumvis.collect())
	# === Timeslots ===
	timeslots = timeslots_handle(broads_input0, broads_input1)
	timeslots.cache()
	# === solve ===
	solve = solve_handle(timeslots)
	solve.cache()
	broads_input2 = sc.broadcast(solve.collect())
	# === correct + Subtract Visibility + Flag ===
	cor_subvis_flag = cor_subvis_flag_handle(broads_input0, broads_input1, broads_input2)
	cor_subvis_flag.cache()
	broads_input = sc.broadcast(cor_subvis_flag.collect())

	# === Gridding Kernel Update + Phase Rotation + Grid + FFT + Rreprojection ===
	grikerupd_pharot_grid_fft_rep = grikerupd_pharot_grid_fft_rep_handle(broads_input_telescope_data, broads_input)
	grikerupd_pharot_grid_fft_rep.cache()
	# ===Sum Facets ===
	sum_facets = sum_facets_handle(grikerupd_pharot_grid_fft_rep)
	sum_facets.cache()
	# === Identify Component ===
	identify_component = identify_component_handle(sum_facets)
	# === Source Find ===
	source_find = source_find_handle(identify_component)
	# === Substract Image Component ===
	subimacom = subimacom_handle(sum_facets, identify_component)
	# === Update LSM ===
	update_lsm = update_lsm_handle(local_sky_model, source_find)
	# # === Terminate ===
	print("Finishing...")
	print("Subtract Image Component: %d" % subimacom.count())
	print("Update LSM: %d" % update_lsm.count())

