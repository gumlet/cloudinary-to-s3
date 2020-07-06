import os
import re
import tqdm
import requests
import argparse
import concurrent.futures
import cloudinary
import cloudinary.uploader
import cloudinary.api
import boto3


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

def get_cloudinary_resource_list(resource_types):

    try:
        resource_types = resource_types.split(",")

        resource_url_list_dict = {}

        for rs_type in resource_types:
            rs_type = rs_type.strip()
            resources = cloudinary.api.resources(resource_type=rs_type)
            if resources and resources["resources"]:
                resource_url_list_dict[rs_type] = []
                resource_url_list_dict[rs_type].extend(
                    list(map(lambda x: x["url"], resources["resources"]))
                )
                if "next_cursor" in resources:
                    next_cursor = resources["next_cursor"]
                    while True:
                        new_resources = cloudinary.api.resources(
                            resource_type=rs_type, next_cursor=next_cursor
                        )
                        if "next_cursor" in new_resources:
                            next_cursor = new_resources["next_cursor"]
                            resource_url_list_dict[rs_type].extend(
                                list(map(lambda x: x["url"], new_resources["resources"]))
                            )
                        else:
                            break
        return resource_url_list_dict            
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
    source_to_target_map = []
    for url in url_list:
        if not keep_same_structure:
            url_split = re.split(r"(\bimage\b|\bvideo\b|\braw\b)/upload/v[0-9]+/", url)[-1]
        else:
            url_split = "/".join(url.split("/")[3:])
        if parent_path:
            url_split = parent_path.strip("/") + "/" + url_split
        source_to_target_map.append([url, url_split])
    return source_to_target_map                

def show_mapping(source_to_target_mapper_dict, s3_endpoint_url, s3_bucket_name):
    for key, value in source_to_target_mapper_dict.items():
        print("Mapping {}: Soruce URL => Target URL".format(key))
        print("------------------------------------------------------------------------------------")
        for source, target in value:
            target = s3_endpoint_url.strip("/") + "/" + s3_bucket_name + "/" + target
            print("{} ==> {}".format(source,target))
        print("\n")    

def migrate_data(data, s3_client, s3_bucket_name, session):
    source, target = data
    try:
        res = session.get(source)
    except Exception as get_err:
        return True, str(get_err)

    try:
        put_resp = s3_client.meta.client.put_object(
            Body=res.content,
            Bucket=s3_bucket_name,
            Key=target
        )
    except Exception as put_err:
        return True, str(put_err)
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

    cloudinary_resource_urls_dict = get_cloudinary_resource_list(args.resource_types)

    if not cloudinary_resource_urls_dict:
        print("No resources to be migrated.")

    source_to_target_mapper_dict = {}

    for rs_type, url_list in cloudinary_resource_urls_dict.items():
        if args.source_buckets:
            cloudinary_resource_urls_dict[rs_type] = filter_urls_base_on_folder_names(
                args.source_buckets,
                url_list
            )
        source_to_target_mapper_dict[rs_type] = source_to_target_mapper(
            url_list,
            args.keep_cloud_name_in_path,
            args.target_parent_path
        )

    show_mapping(
        source_to_target_mapper_dict,
        args.s3_endpoint_url,
        args.s3_bucket_name)

    with requests.Session() as sess:
        for rs_type, data_list in source_to_target_mapper_dict.items():
            results = []
            failed_count = 0
            print("Migrating {} {} type resources.".format(len(data_list), rs_type))
            print("------------------------------------------------------------------------------------")
            for data in tqdm.tqdm(data_list):
                err, resp = migrate_data(data, s3, args.s3_bucket_name, sess)
                if err:
                    failed_count += 1
                results.append([err, resp, data[0]])
            print("Migrated {}/{} {} type resources. \n".format(len(data_list)-failed_count, len(data_list), rs_type))

            if failed_count > 0:
                print("Following resources failed:")
                for r in results:
                    if r[0]:
                        print("source URL: {}, Error Message: {}".format(r[3], r[2]))


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
        help="Region name for s3 bucket."
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
        "-v",
        "--verbose",
        type=bool,
        default=False,
        help="verbosity for script."
    )

    args = parser.parse_args()

    run(args)
