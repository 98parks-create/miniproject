import os
import json
import csv
from django.core.management.base import BaseCommand
from youth_road.firebase_service import FirebaseManager

class Command(BaseCommand):
    help = 'Bulk upload housing/welfare data with intelligent deduplication'

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help='Path to the JSON or CSV file')
        parser.add_argument('category', type=str, choices=['housing', 'welfare'], help='Category: housing or welfare')

    def handle(self, *args, **options):
        file_path = options['file_path']
        category = options['category']
        collection_name = 'housing_notices' if category == 'housing' else 'welfare_policies'
        
        # ID 필드 매핑
        id_field = 'id' if category == 'housing' else 'id'

        if not os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f'File not found: {file_path}'))
            return

        data_list = []
        try:
            if file_path.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    data_list = json.load(f)
            elif file_path.endswith('.csv'):
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    data_list = list(reader)
            
            if not data_list:
                self.stdout.write(self.style.WARNING('No data to upload.'))
                return

            self.stdout.write(f'🚀 Processing {len(data_list)} items for {category}...')
            
            # Firebase Sync (FirebaseManager.sync_data가 내부적으로 set()을 써서 ID 중복 시 덮어쓰기함)
            success = FirebaseManager.sync_data(collection_name, data_list, id_field=id_field)
            
            if success:
                self.stdout.write(self.style.SUCCESS(f'✅ Successfully uploaded/updated {len(data_list)} items in {collection_name}.'))
            else:
                self.stdout.write(self.style.ERROR('❌ Failed to sync with Firebase.'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error during bulk upload: {e}'))
