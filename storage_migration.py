import os
import re
from tqdm import tqdm
import requests
import argparse
import cloudinary
import cloudinary.uploader
import cloudinary.api
import boto3
import concurrent.futures

def set_cloudinary_config(cloud_name, api_key, api_secret):
    try:
        cloudinary.config(cloud_name=cloud_name, api_key=api_key, api_secret=api_secret)
        return False, ""
    except Exception as err:
        return True, str(err)

def create_s3_resource_client(s3_endpoint_url, s3_access_key_id, s3_secret_access_key):
    try:
        s3 = boto3.resource(
            "s3",
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
        )
        return False, s3
    except Exception as err:
        return True, str(err)

def migrate_cloudinary_resources(resource_types, s3):

    try:
        resource_types = resource_types.split(",")
        source_buckets = args.source_buckets.split(",")
        batch_index = 0
        batch_size = 500
        resource_url_list_dict = {}

        with requests.Session() as sess:
            with concurrent.futures.ThreadPoolExecutor(max_workers = args.max_worker) as executor:

                for rs_type in resource_types:
                    rs_type = rs_type.strip()

                    resources = cloudinary.api.resources(resource_type=rs_type,max_results=batch_size)
                    while resources and resources["resources"]:
                        batch_index += 1
                        current_batch = list(map(lambda x: x["url"], resources["resources"]))
                        if args.source_buckets:
                            current_batch = filter_urls_base_on_folder_names(source_buckets,current_batch)

                        source_to_target_mapper_list = source_to_target_mapper(
                            current_batch,
                            args.keep_cloud_name_in_path,
                            args.target_parent_path
                        )

                        if batch_index == 1:
                            show_sample_mapping(
                                source_to_target_mapper_list,
                                args.s3_endpoint_url,
                                args.s3_bucket_name)

                            confirm = input("Please check your input data once and confirm : [yes]")
                            if confirm.lower() != 'yes':
                                break
                            print("\n")

                        print("Migrating batch {} ...".format(batch_index))
                        #start migrating....
                        pbar = tqdm(total=len(source_to_target_mapper_list))
                        progress = 0
                        failed_count = 0
                        results = []

                        future_to_data = {executor.submit(migrate_data, data, s3, args.s3_bucket_name, sess): data for data in source_to_target_mapper_list}
                        for future in concurrent.futures.as_completed(future_to_data):
                            progress += 1
                            pbar.update(progress)
                        try:
                            err,resp = future.result()
                        except Exception as ex:
                            print("Exception : ",ex)

                        if err:
                            failed_count += 1
                            results.append(resp)

                        pbar.close()

                        if failed_count > 0:
                            print("Following resources failed:")
                            for r in results:
                                if r[0]:
                                    print("source URL: {}".format(r))

                        # get the next batch....
                        if "next_cursor" in resources:
                            resources = cloudinary.api.resources(resource_type=rs_type, max_results=batch_size, next_cursor=resources["next_cursor"])
                        else:
                            break

    except Exception as err:
        print("Error ocurred in getting cloudinary resource URLs: {}".format(str(err)))
        return {}

def filter_urls_base_on_folder_names(source_buckets, url_list):
    source_buckets = source_buckets.split(",")
    filtered_urls = []
    for sb in source_buckets:
        sb = sb.strip()
        for url in url_list:
            if sb in url:
                filtered_urls.append(url)
    return list(set(filtered_urls))

def source_to_target_mapper(url_list, keep_same_structure, parent_path):
    source_to_target_list = []
    for url in url_list:
        if not keep_same_structure:
            url_split = re.split(r"(\bimage\b|\bvideo\b|\braw\b)/upload/v[0-9]+/", url)[-1]
        else:
            url_split = "/".join(url.split("/")[3:])
        if parent_path:
            url_split = parent_path.strip("/") + "/" + url_split
        source_to_target_list.append([url, url_split])
    return source_to_target_list

def show_sample_mapping(source_to_target_mapper_dict, s3_endpoint_url, s3_bucket_name):
    print("\n#Sample Mapping")
    print("------------------------------------------------------")
    for source,target in source_to_target_mapper_dict[:5]:
        target = s3_endpoint_url.strip("/") + "/" + s3_bucket_name + "/" + target
        print("{} ==> {}".format(source,target))

    print("\n")

def migrate_data(data, s3_client, s3_bucket_name, session):
    source, target = data

    if args.resuming_migration:
        #ignore if already uploaded to s3...
        try:
            s3_object_meta = s3_client.meta.client.head_object(
                Bucket = s3_bucket_name,
                Key = target
            )
        except Exception as head_err:
            s3_object_meta = None

        if s3_object_meta:
            return False, data

    try:
        res = session.get(source)
    except Exception as get_err:
        return True, data

    try:
        put_resp = s3_client.meta.client.put_object(
            Body=res.content,
            Bucket=s3_bucket_name,
            Key=target
        )
    except Exception as put_err:
        return True, data
    return False, put_resp

def run(args):

    err, msg = set_cloudinary_config(
        args.cloudinary_cloud_name,
        args.cloudinary_api_key,
        args.cloudinary_api_secret,
    )

    if err:
        print("Error {} ocurred in connecting cloudinary. check config.".format(msg))
        return

    err, s3 = create_s3_resource_client(
        args.s3_endpoint_url,
        args.s3_access_key_id,
        args.s3_secret_access_key,
    )

    if err:
        print("Error {} ocurred in creating s3 client. check config.".format(s3))
        return

    migrate_cloudinary_resources(args.resource_types,s3)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        "Script to migrate cloudinary resources to S3 storage."
    )

    parser.add_argument(
        "--cloudinary_cloud_name",
        type=str,
        required=True,
        help="souce cloudinary cloud name.",
    )
    parser.add_argument(
        "--cloudinary_api_key",
        type=str,
        required=True,
        help="source cloudinary API key.",
    )
    parser.add_argument(
        "--cloudinary_api_secret",
        type=str,
        required=True,
        help="source cloudinary API secret.",
    )
    parser.add_argument(
        "--s3_endpoint_url",
        type=str,
        required=True,
        help="S3 endpoint url"
    )
    parser.add_argument(
        "--s3_access_key_id",
        type=str,
        required=True,
        help="S3 access key."
    )
    parser.add_argument(
        "--s3_secret_access_key",
        type=str,
        required=True,
        help="S3 secret key."
    )
    parser.add_argument(
        "--s3_bucket_name",
        type=str,
        required=True,
        help="S3 target bucket name."
    )
    parser.add_argument(
        "--resource_types",
        type=str,
        default="image,raw,video",
        help="comma seperated list of resource types (image, raw, video) to be migrated.",
    )
    parser.add_argument(
        "--keep_cloud_name_in_path",
        type=bool,
        default=False,
        help="When kept this argument True it script will create target structure with 'cloud_name/:resource_type/upload' \
            otherwise (default case) target structure will be same as you can see in your cloudinary Media Library.",
    )
    parser.add_argument(
        "--source_buckets",
        type=str,
        default="",
        help="Given comma seperated list of buckets/folder names, script will only migrate resource following \
            bucket names/folder names. Provide only absolute paths.(format: '/samples/rats,/samples/birds')",
    )
    parser.add_argument(
        "--target_parent_path",
        type=str,
        default="",
        help="Target structure will be created following specified path. (prefix path)",
    )

    parser.add_argument(
        "--resuming_migration",
        type=bool,
        default=False,
        help="When kept this argument true it will skip the object already migrated to s3.",
    )

    parser.add_argument(
        "--max_worker",
        type=int,
        default=25,
        help="It limits maximum concurrent threads created during migration of resources.",
    )

    args = parser.parse_args()


    run(args)
