#699 mb @ 11:16 Mon Jan 9
#from urllib import response -Doesn't seem like we need this
import galaxy_api_class
import box_api_class
import os
import tifffile
import hashlib
import logging, requests
from PIL import Image
import logging.handlers
from datetime import date

# from outsource_IO import FAILED_DOWNLOADS

logging.basicConfig(filename = "pathing_IO.log",encoding = 'utf-8', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level = logging.DEBUG)
# smtp_handler = logging.handlers.SMTPHandler(mailhost=("smtp.gmail.com",465), fromaddr="bkilleen@rh.com",toaddrs=["bkilleen@rh.com","landerson2@rh.com"], subject="Derp", credentials=["bkilleen@rh.com","rR4$5698"])
# smtp_handler.setLevel(logging.DEBUG)

PRODUCTION = True # set to true to move to production for downloads
SUCCESSFUL_UPLOADS = []
SUCCESSFUL_DOWNLOADS = []
FAILED_UPLOADS = []
FAILED_DOWNLOADS = []
TEMP_FOLDER = os.path.expanduser("~/Downloads/")
# UPLOAD_FOLDER = 102870413884 ## the folder that uploads will land in - testing folder
UPLOAD_FOLDER = 229870305597
DOWNLOAD_FOLDER = 96164790463 # the root folder for all assets returning

logging.debug('Pathing IO Galaxy Class initiated')

def remove_alphas(file_path):
	#take a string for a filepath and return a PIL.Image without alphas
	logging.debug(f'Remove alphas: {file_path}')
	im = tifffile.imread(file_path)
	sliced_npy_array = im[:, :, 0]
	# print(sliced_npy_array.shape)
	# print(sliced_npy_array[0], flush = True)
	data = Image.fromarray(sliced_npy_array)
	return data

def convert_file_to_jpg(path):
	# take a filepath and convert to jpg and return a bytes object
	logging.debug(f'Converting {path} to JPG')
	tempfile = os.path.join(TEMP_FOLDER,os.path.basename(path).replace(".tif",'.jpg'))
	try:
		try: 
			tif = Image.open(path)
		except:
			tif = remove_alphas(path)
		with tif.convert('RGB') as tif:
			tif.save(tempfile, format = "JPEG", dpi = (300,300)) 
	except FileNotFoundError:
		logging.error(f"pathing_IO.convert_file_to_jpg({path}) :: Unable to turn into JPG -- File Not Found")
		return None
	logging.info(f"pathing_IO.convert_file_to_jpg({path}) :: {path} converted to JPG")
	return tempfile

# Requires that .Exiftool_config is present in Home and contains custom metadata fields (for EntryID)
def add_rh_metadata(path, tag, value):
	command = f"exiftool -{tag}={value} {path}"
	os.system(command)

## find items that are not retouch
def upload_protocol():
	logging.info("pathing_IO.upload_protocol() :: Initiating upload protocol")
	global UPLOAD_FOLDER
	## need to add folder creation like "RH_WEB_011923_Product_Outlines_A_65" RH_WEB_MMDDYY_Product_Outlines_A_#ofFiles
	params = {
		'query':[{
			'RetouchStatus':'Not Retouch',
			'RetoucherName':'=',
			'omit': "false",
		},
		{
			'cRetoucher_ ImageName':'*_CC.tif',
			'omit': "true",
		},
		{
			'cRetoucher_ ImageName':'*_XBC*',
			'omit': "true",
		},
		{
			'cRetoucher_ ImageName':'*_Frank*',
			'omit': "true",
		},
		{
			'cRetoucher_ ImageName':'*_Synth*',
			'omit': "true",
		}
	]
}
	upload_paths = [] # paths to upload
	gx_response = gx.find_records(params)
	if 'response' in gx_response:
		logging.debug("pathing_IO.upload_protocol() :: GX response received")
		response = gx_response['response']
		if 'data' in response:
			for record in response['data']:
				# print(record['fieldData'].keys())
				# break
				record_data = record['fieldData'] # the GX field data
				# print(record_data['ImagePath'])
				# we now have a list of GX Records with a Retouch Status of 'Not Retouch'
				upload_paths.append((record['recordId'], record_data['ImagePath'], record_data['EntryID']))

	## upload the paths
	
	if len(upload_paths) > 0 :
		## generate a destination folder
		# "RH_WEB_011923_Product_Outlines_A_65" RH_WEB_MMDDYY_Product_Outlines_A_#ofFiles
		today = str(date.today())
		day, month, year = today[8:],today[5:7],today[:4]
		folder_name = f"RH_WEB_{month}{day}{year[-2:]}_Product_Outlines_A_{len(upload_paths)}"
		response = box.create_folder(UPLOAD_FOLDER,folder_name)
		
		if not response == None:
			UPLOAD_FOLDER = response
	logging.info(f"pathing_IO.upload_protocol() :: Uploading {len(upload_paths)} files for pathing to {UPLOAD_FOLDER}...")
	
	def write_to_manifest(data):
		logging.debug(f"pathing_IO.upload_protocol() :: Writing data to manifest...")
		with open("upload_manifest.csv", "a") as csv:
			csv.write(data+"\n")
		return
	## clear manifest and write headers to manifest
	with open("upload_manifest.csv", "w") as csv:
		csv.write("")
	write_to_manifest("project,filename")
	logging.info("pathing_IO.upload_protocol() :: Initial upload_manifest generated")

	for _path in upload_paths:
		logging.debug(f"pathing_IO.upload_protocol() :: Uploading {_path} to Box")
		logging.debug(f"pathing_IO.upload_protocol() :: '_path' object is of type {type(_path)}")
		path = _path[1].replace(":","/").strip()
		logging.debug(f"pathing_IO.upload_protocol() :: 'Path' is now {path}")
		path = f"/Volumes/{path}"
		logging.info(f"Uploading {path} to Box")
		if path == "/Volumes/": 
			logging.error(f"pathing_IO.upload_protocol() :: Path for record {_path[0]} converted to /Volumes/ and failed to upload. Skipping")
			continue
		if not os.path.exists(path):
			logging.error(f'pathing_IO.upload_protocol() :: Pathing_IO :: File not found {path}')
			FAILED_UPLOADS.append(_path)
			continue
		# next two lines are JPG workflow
		# check if the file already exists in the folder
		params = {
			'type':'file',
			'ancestor_folder_ids' : [UPLOAD_FOLDER],
			'fields': 'name',
			'query' : os.path.basename(path).replace(".tif",".jpg"),
			'limit' : 1
		}
		try:
			jpg = convert_file_to_jpg(path) ## returns the path to the file
		except: 
			jpg = None
		if jpg == None: 
			logging.debug(f"pathing_IO.upload_protocol() :: File failed to convert to JPG successfully")
			FAILED_UPLOADS.append(path)
			continue
		try:
			if box.upload_preflight(jpg,UPLOAD_FOLDER) != 200:
				logging.warning(f"pathing_IO.upload_protocol() :: Upload to box folder {UPLOAD_FOLDER} for {path} returned non-200 preflight")
				continue
			add_rh_metadata(jpg, 'entryID', _path[2])
			logging.info(f"pathing_IO.upload_protocol() :: EntryID {_path[2]} added to outgoing record")
			f = box.upload(jpg,UPLOAD_FOLDER) # need to handle non-200 responses here
		except requests.exceptions.SSLError as err:
			logging.error(f'pathing_IO.upload_protocol() :: SSL error encoutered for {path}.')
			continue
		## check JPG hash against uploaded hash NEEDS REVIEW
		try:
			if hashlib.sha1(open(jpg,"rb").read()).hexdigest() != f.json()['entries'][0]['file_version']['sha1']:
				logging.warning(f"pathing_IO.upload_protocol() :: Failed SHA1 validation for {path}, attempting again.")
				f = box.upload(jpg,UPLOAD_FOLDER)
				if hashlib.sha1(open(jpg,"rb").read()).hexdigest() != f.json()['entries'][0]['file_version']['sha1']:
					logging.error(f"pathing_IO.upload_protocol() :: Failed SHA1 validation for {path}")
					continue
			else:
				logging.debug(f"pathing_IO.upload_protocol() :: Successful SHA1 validation for {path}")
		except:
			logging.error(f"pathing_IO.upload_protocol() :: SHA1 failed for {path}. Skipping.")
			continue

		# check to see if the response from the upload session is a 
			# NoneType or a valid response
			# if the response is valid and is less than 200, add to list of successful files
		if f is not None and type(f) is not type(1) and (f.status_code < 400 or f.status_code == 409):
			logging.info(f"pathing_IO.upload_protocol() :: {path} succesfully uploaded to Box")
			SUCCESSFUL_UPLOADS.append(_path)
			# clean up local JPG files as uploads are complete
			os.remove(jpg) 

	logging.info(f"{len(SUCCESSFUL_UPLOADS)} uploads completed. Updating GX Status'...")
	## update GX data for each succcessful upload
	## tuples in SUCCESSFUL_UPLOADS are (entryid, path)
	for upload in SUCCESSFUL_UPLOADS:
		# update all GX status' for succesful Box uploads 
		project = upload[1].split(":")[3]
		if project in ['ProductLaunches','UnitedStates_US']:
			project = upload[1].split(":")[5]
		if "_" not in project:
			project = upload[1].split(":")[2]
		else:
			project = project.split('_')[1]
		if project == 'US':
			pass
		filename = upload[1].split(":")[-1]
		write_to_manifest(f"{project},{filename}")
		data = {"RetouchStatus":"Out for Pathing"}
		logging.info(f"pathing_IO.upload_protocol() :: updating {upload} in GX with Status 'Out for Pathing'")
		gx.update_record(upload[0],data)
	if len(SUCCESSFUL_UPLOADS) > 0:
		box.upload("upload_manifest.csv",UPLOAD_FOLDER)
		os.remove("upload_manifest.csv")
		logging.info(f"pathing_IO.upload_protocol() :: upload_manifest.csv delivered to {UPLOAD_FOLDER}")
	logging.info("pathing_IO.upload_protocol() :: Galaxy updates complete.")


def download_protocol():
	logging.info("pathing_IO.download_protocol() :: Query Galaxy for path intake...")
	params = {
		'query':[{
			'RetouchStatus':'Out for Pathing',
			'omit': "false"
		}]
	}
	gx_paths = [] # paths to upload
	gx_response = gx.find_records(params)
	if 'response' in gx_response:
		response = gx_response['response']
		if 'data' in response:
			for record in response['data']:
				record_data = record['fieldData'] # the GX field data
				# we now have a list of GX Records with a Retouch Status of 'Not Retouch'
				gx_paths.append((record['recordId'], record_data['ImagePath'], record_data['cRetoucher_ ImageName'], record_data['EntryID']))
	logging.info("pathing_IO.download_protocol() :: Galaxy intake records located. Attempting to ingest from Box")
	## attempt to find the files on Box
	for i in gx_paths:
		extension = "_m.jpg"
		box_files = box.search_filename(i[2].replace(".tif",extension), folder = '96164790463', exclusions='96165013049')
		if box_files == None or box_files == []: 
			logging.debug(f"pathing_IO.download_protocol() :: Pathing IO attempting to find PSD for {i[2]}")
			extension = "_m.psd"
			box_files = box.search_filename(i[2].replace(".tif",extension), folder = '96164790463', exclusions='96165013049')
			if box_files == None or box_files == []:
				logging.warning(f"pathing_IO.download_protocol() :: Pathing IO unable to find PSD or JPG for {i[2]}")
				continue
		logging.debug(f"pathing_IO.download_protocol() :: pathing IO found assets on Box for {i[2]}\n\t{box_files}\n")
		if type(box_files) == list:
			if len(box_files) == 1: box_files = box_files[0]
			else:
				for file in box_files:
					if file['name'][:-6] == i[2][:-4]:
						box_files = file

		
		if PRODUCTION:
			# download to webnative
			destination = i[1].replace("Processed","Outlines").replace(".tif",extension).replace(":","/")
			logging.info(f"pathing_IO.download_protocol() :: Downloading {i[2]} from Box to {destination}")
			if "/Volumes" not in destination: destination = f"/Volumes/{destination}"
			dlRes = box.download_files(box_files, destination)
			# confirm download is correct via hash
			if dlRes is not None:
				dlRes_hash = hashlib.sha1(open(dlRes,"rb").read()).hexdigest()
				# if hashlib.sha1(open(dlRes,"rb").read()).hexdigest() != box_files.json()[0]['file_version']['sha1']:
				if type(box_files) == list: 
					for i,box_file in enumerate(box_files):
						if dlRes_hash == box_file['file_version']['sha1']:
							box_files = box_file
							break
				if type(box_files) == list:
					logging.error(f"pathing_IO.upload_protocol() :: Error downloading file {gx_paths[i][-2]}")
					FAILED_DOWNLOADS.append(i)
					continue
				logging.info(f"pathing_IO.upload_protocol() :: Successfully downloaded {i[0]} to {destination}")
				temp = list(i)
				temp[1] = destination.replace("/Volumes/","").replace('/',":")
				SUCCESSFUL_DOWNLOADS.append(temp)
		else:
			destination = i[1]
			logging.info(f"pathing_IO.download_protocol() :: Downloading {i[2]} from Box to {destination}")
			if box.download_files(box_files[0]['id']) is not None:
				SUCCESSFUL_DOWNLOADS.append(i)

	logging.info(f"pathing_IO.download_protocol() :: {len(SUCCESSFUL_DOWNLOADS)} Files downloaded.")

	for i in SUCCESSFUL_DOWNLOADS:
		logging.info("pathing_IO.download_protocol() :: Updating Galaxy...")
		data = {"RetouchStatus":"Ready for Retouching",
			"OutlinePath" : i[1].replace(".tif",extension).replace("Processed","Outlines")
			}
		if gx.update_record(i[0],data).status_code < 400:
			logging.info(f"pathing_IO.download_protocol() :: Updated data for {i[2]}")


gx = None
box = None

def main():
	global gx
	global box
	# Uncomment this for production
	gx = galaxy_api_class.gx_api(production=True)
	# gx = galaxy_api_class.gx_api()
	box = box_api_class.box_api()
	download_protocol()
	upload_protocol()
	logging.info("pathing_IO.main() :: Completed")
if __name__ == "__main__": main()