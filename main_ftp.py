import os
import re
import gzip
import urllib.request
from pymongo import MongoClient
import xmltodict
import argparse
import ftplib
import pubmed_parser as pp
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import cProfile
import pstats
import io

load_dotenv()


def setup_logger():
    """
    Set up a logger for the script.

    Returns:
        logger: A logging.Logger object.
    """
    logger = logging.getLogger('pubmed_retrieve')
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('pubmed_retrieve.log')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def send_email(email_from, email_to, gmail_key, error_message, logger):
    """
    Sends an email with an error message when there's an exception.

    Args:
        email_from (str): The sender's email address.
        email_to (str): The recipient's email address.
        gmail_key (str): The sender's Gmail password.
        error_message (str): The error message to send.
        logger (logging.Logger): The logger object.
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = email_to
        msg['Subject'] = 'There was an error with pubmed retrieve'

        body = error_message
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(email_from, gmail_key)
        text = msg.as_string()
        server.sendmail(email_from, email_to, text)
        server.quit()
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email due to: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


def get_db_collection(logger):
    """
    Connects to the MongoDB and returns the collection.

    Args:
        logger (logging.Logger): The logger object.

    Returns:
        collection: The MongoDB collection.
    """
    try:
        client = MongoClient('localhost', 27017)
        db = client['pubmed']
        collection = db['ajustes']
        return collection
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_db_collection: {e}")
        send_email(os.getenv('email_from'), os.getenv('email_to'),
                   os.getenv('gmail_key'), str(e), logger)
        raise e


def get_last_file(collection, logger):
    """
    Get the last file downloaded from the FTP server.

    Args:
        collection: The MongoDB collection.
        logger (logging.Logger): The logger object.

    Returns:
        last_file (str): The last file downloaded from the FTP server.
    """
    try:
        last_document = collection.find_one()
        if last_document:
            return last_document['filename']
        else:
            return None
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_last_file: {e}")
        send_email(os.getenv('email_from'), os.getenv('email_to'),
                   os.getenv('gmail_key'), str(e), logger)
        raise e


def parse_filename(filename, logger):
    """
    Parse the filename to extract the file number.

    Args:
        filename (str): The filename.
        logger (logging.Logger): The logger object.

    Returns:
        file_number (int): The file number.
    """
    try:
        return int(re.findall('n(\d+)\.', filename)[0])
    except Exception as e:
        logger.error(f"An unexpected error occurred in parse_filename: {e}")
        send_email(os.getenv('email_from'), os.getenv('email_to'),
                   os.getenv('gmail_key'), str(e), logger)
        raise e


def download_file(url, filename, logger):
    """
    Download a file from the FTP server.

    Args:
        url (str): The file url.
        filename (str): The filename.
        logger (logging.Logger): The logger object.
    """
    try:
        urllib.request.urlretrieve(url, filename)
    except Exception as e:
        logger.error(f"An unexpected error occurred in download_file: {e}")
        send_email(os.getenv('email_from'), os.getenv('email_to'),
                   os.getenv('gmail_key'), str(e), logger)
        raise e


def decompress_file(input_filename, output_filename, logger):
    """
    Decompress a .gz file.

    Args:
        input_filename (str): The .gz file.
        output_filename (str): The output file.
        logger (logging.Logger): The logger object.
    """
    try:
        with gzip.open(input_filename, 'rb') as f_in:
            with open(output_filename, 'wb') as f_out:
                f_out.write(f_in.read())
    except Exception as e:
        logger.error(f"An unexpected error occurred in decompress_file: {e}")
        send_email(os.getenv('email_from'), os.getenv('email_to'),
                   os.getenv('gmail_key'), str(e), logger)
        raise e


def list_files(ftp, logger):
    """
    List all files in the FTP directory.

    Args:
        ftp: The FTP connection.
        logger (logging.Logger): The logger object.

    Returns:
        files (list): The list of files.
    """
    files = []
    try:
        files = ftp.nlst()
    except ftplib.error_perm as resp:
        if str(resp) == "550 No files found":
            logger.info("No files in this directory")
        else:
            logger.error(f"An error occurred in list_files: {resp}")
            send_email(os.getenv('email_from'), os.getenv(
                'email_to'), os.getenv('gmail_key'), str(resp), logger)
            raise resp
    return files


def update_database(collection, file_path, logger):
    """
    Update the database with new articles from a file.

    Args:
        collection: The MongoDB collection.
        file_path (str): The file path.
        logger (logging.Logger): The logger object.
    """
    try:
        dicts_out = pp.parse_medline_xml(
            file_path, year_info_only=False, nlm_category=True, author_list=True, reference_list=True)
        for article in dicts_out:
            existing_article = collection.find_one({'_id': article['pmid']})
            new_references = set(article['reference'].split(';'))
            reference_docs = [{'pmid': ref, 'idArticle': None}
                              for ref in new_references]

            if existing_article:
                old_references = set(ref['pmid']
                                     for ref in existing_article.get('references', []))
                if old_references != new_references:
                    article['updated'] = True
                    article['references'] = reference_docs
                    collection.update_one(
                        {'_id': existing_article['_id']}, {'$set': article})
                else:
                    article['updated'] = False
            else:
                article['updated'] = True
                article['references'] = reference_docs
                collection.insert_one(article)

        # Update 'idArticle' field in 'references' array for all articles with 'updated' = True
        updated_articles = collection.find({'updated': True})
        for article in updated_articles:
            for ref in article['references']:
                ref_article = collection.find_one({'_id': ref['pmid']})
                if ref_article:
                    ref['idArticle'] = ref_article['_id']
            collection.update_one({'_id': article['_id']}, {
                                  '$set': {'references': article['references'], 'updated': False}})
    except Exception as e:
        logger.error(f"An unexpected error occurred in update_database: {e}")
        send_email(os.getenv('email_from'), os.getenv('email_to'),
                   os.getenv('gmail_key'), str(e), logger)
        raise e


def main(host, directory, remote_dir):
    """
    The main function.

    Args:
        host (str): The FTP host.
        directory (str): The local directory to store the downloaded files.
        remote_dir (str): The FTP remote directory to download files from.
    """
    logger = setup_logger()
    pr = cProfile.Profile()
    pr.enable()

    try:
        collection = get_db_collection(logger)
        last_file = get_last_file(collection, logger)
        last_number = parse_filename(last_file, logger) if last_file else 0

        ftp = ftplib.FTP(host)
        ftp.login()
        ftp.cwd(remote_dir)

        files = list_files(ftp, logger)
        files_to_download = [
            file for file in files if parse_filename(file, logger) > last_number]

        for file in sorted(files_to_download, key=lambda x: parse_filename(x, logger)):
            gz_file_name = os.path.join(directory, file)
            xml_file_name = gz_file_name[:-3]

            download_file('ftp://' + host + remote_dir +
                          file, gz_file_name, logger)
            decompress_file(gz_file_name, xml_file_name, logger)
            update_database(collection, xml_file_name, logger)

        ftp.quit()

        # delete all files in directory at the end
        for filename in os.listdir(directory):
            os.remove(os.path.join(directory, filename))

    except Exception as e:
        logger.error(f"An unexpected error occurred in main: {e}")
        send_email(os.getenv('email_from'), os.getenv('email_to'),
                   os.getenv('gmail_key'), str(e), logger)
        raise e

    finally:
        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        logger.info(s.getvalue())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update PubMed database")
    parser.add_argument(
        'host', type=str, help='The FTP host to download files from', default='ftp.ncbi.nlm.nih.gov')
    parser.add_argument('remote_dir', type=str,
                        help='The FTP remote directory to download files from', default='/pubmed/updatefiles')
    parser.add_argument('directory', type=str,
                        help='The local directory to store downloaded files', default='xmL_data')
    args = parser.parse_args()

    main(args.host, args.directory, args.remote_dir)
