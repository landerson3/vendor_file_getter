import sys, os
sys.path.insert(0,os.path.expanduser('~/Galaxy_Box_Integration'))

from box_api_class import box_api
from galaxy_api_class import gx_api


class file_getter:
	def __init__(self):
		# REQ TO RUN:
		'''
		connected to internet
		connected to webnative
		'''
		# start box API
		self.box = box_api()
		# define vendor folders
		self.vendor_folder_ids = { # fill out later with box folder IDs for vendors
			'Happy Finish': 75985820687,
			'SGK': 155228682132,
			'ICS': 205190710317
		}
		# define known folder exclusions
		self.set_vendor_exculsions()
		# get files from vendor folders
		self.get_vendor_file_lists()
		# start GX API
		self.galaxy = gx_api(production = True)
		# get wips & processed paths for folders
		# check GX file assignment against Box return vendor
		self.get_gx_files()
		self.match_files() 
		# upload assets to webnative
		self.upload_wips()
		self.update_gx()
		self.move_box_files()

	def move_box_files(self):
		for asset in self.successful_uploads:
			for match in self.matches:
				if match['record_id'] == asset:
					pulled_folder = self.pulled_folders[match['vendor']][0]
					file_id = match['id']
					self.box.move_file(file_id, pulled_folder)
					
	def update_gx(self):
		# update status' to "approved" in GX
		for upload in self.successful_uploads:
			self.galaxy.update_record(upload,{"RetouchStatus":"Approved"})

	def upload_wips(self):
		# upload the files from the match sets by downloading the file id and posting to the webnative path
		self.successful_uploads = []
		for match in self.matches:
			if self.box.download_files(match['id'],match['path']) != None: 
				self.successful_uploads.append(match['record_id'])


	def match_files(self) -> None:
		# match up files on the gx_files and vendor_files variables and return a tuple of (box_file_id, wips_path)
		self.matches = []
		for vendor in self.vendor_files:
			for file in self.vendor_files[vendor]:
				for gx_file in self.gx_files:
					if file['name'].lower()[:-4] == gx_file['name'].lower()[:-4]:
						if gx_file['wips_path'] in (None, "", []):
							path = gx_file['processed_path'].replace("Processed","WIPS").replace(".tif",'.psb')
						else: path = gx_file['wips_path']
						self.matches.append(
							{
								'id': file['id'],
								'path': path,
								'record_id': gx_file['record_id'],
								'vendor': vendor
							}
							)
		
		
	def get_gx_files(self) -> None:
		res = {}
		for vendor in self.vendor_files:
			res[vendor] = []
			files = []
			params = {
				'query':[]
			}
			for f in self.vendor_files[vendor]:
				if f['name'][-4:].lower() == '.psb':
					files.append(f)
			for file in files:
				p = {
					'cRetoucher_ImageName':file['name'][:-3],
					'RetouchStatus':'In Revisions',
					'RetoucherName':vendor,
					'omit': "false"
				}
				params['query'].append(p)
			gx_res = self.galaxy.find_records(params = params)
			if 'response' in gx_res and 'data' in gx_res['response']:
				res[vendor].extend(gx_res['response']['data'])
		self.gx_files = []
		for vendor in res:
			for file in res[vendor]:
				self.gx_files.append(
					{
						'name': file['fieldData']['ImageName'],
						'processed_path': file['fieldData']['ImagePath'],
						'wips_path': file['fieldData']['WIPS_PATH'],
						'final_path': file['fieldData']['FINAL_PATH'],
						'record_id': file['recordId']
					}
				)
		

	def set_vendor_exculsions(self) -> None:
		# define a list of folders to exluce from the vendor folders
		self.folder_exlusions = {}
		self.pulled_folders = {}
		for name, id in self.vendor_folder_ids.items():
			if name not in self.folder_exlusions:
				self.folder_exlusions[name] = []
				self.pulled_folders[name] = []
			items = self.box.get_folder_items(id, include_subfolders = False)
			for item in items:
				lname = item['name'].lower()
				if 'pulled' in lname or 'requested' in lname:
					self.folder_exlusions[name].append(item['id'])
					if 'pulled' in lname:
						self.pulled_folders[name].append(item['id'])


	def get_vendor_file_lists(self) -> None:
		# get the file lists for download while omitting the vendors requested and pulled folders
		# set the self.vendor_files object to hold the files by each vendor
		self.vendor_files = {}
		for name,id in self.vendor_folder_ids.items():
			files = self.box.get_folder_items(id, exclusions = self.folder_exlusions[name]) 
			if name not in self.vendor_files: self.vendor_files[name] = files
			self.vendor_files[name]

fg = file_getter()
