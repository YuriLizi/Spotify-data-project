"""
RSS Feed Parser Module

This module takes podcast  feed XML files and converts them to a tabular structure .
extracts podcast metadata, episode details, and other information
"""

import os
import pandas as pd
import xml.etree.ElementTree as ET
import logging
from datetime import datetime
import re
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_podcast_metadata(root):
    """
    Extract podcast-level metadata from RSS feed

    Args:
        root: XML root element

    Returns:
        dict: Podcast metadata
    """
    channel = root.find('channel')

    # Define namespace for iTunes-specific elements
    ns = {'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}

    metadata = {}

    # Basic podcast info
    metadata['title'] = get_element_text(channel, 'title')
    metadata['description'] = get_element_text(channel, 'description')
    metadata['link'] = get_element_text(channel, 'link')
    metadata['language'] = get_element_text(channel, 'language')
    metadata['last_build_date'] = get_element_text(channel, 'lastBuildDate')

    # iTunes specific metadata
    metadata['author'] = get_element_text(channel, 'itunes:author', ns)
    metadata['category'] = get_element_text(channel, 'itunes:category', ns, attr='text')
    metadata['explicit'] = get_element_text(channel, 'itunes:explicit', ns)
    metadata['image'] = get_element_text(channel, 'itunes:image', ns, attr='href')

    # Additional metadata
    metadata['copyright'] = get_element_text(channel, 'copyright')
    metadata['generator'] = get_element_text(channel, 'generator')

    logger.info(f"Extracted metadata for podcast: {metadata.get('title', 'Unknown')}")
    return metadata


def extract_episodes(root):
    """
    Extract episode details from RSS feed

    Args:
        root: XML root element

    Returns:
        list: List of episode dictionaries
    """
    channel = root.find('channel')
    items = channel.findall('item')
    ns = {'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}

    episodes = []

    for item in items:
        episode = {}

        # Basic episode info
        episode['title'] = get_element_text(item, 'title')
        episode['description'] = get_element_text(item, 'description')
        episode['pub_date'] = get_element_text(item, 'pubDate')

        # Clean and convert pub_date to datetime
        if episode['pub_date']:
            try:
                dt = parse_date(episode['pub_date'])
                episode['pub_date_iso'] = dt.isoformat() if dt else None
            except Exception as e:
                logger.warning(f"Error parsing date for {episode['title']}: {e}")
                episode['pub_date_iso'] = None

        # Enclosure information (the actual media file)
        enclosure = item.find('enclosure')
        if enclosure is not None:
            episode['media_url'] = enclosure.attrib.get('url')
            episode['media_length'] = enclosure.attrib.get('length')
            episode['media_type'] = enclosure.attrib.get('type')

        # iTunes specific metadata
        episode['duration'] = get_element_text(item, 'itunes:duration', ns)
        episode['episode_number'] = get_element_text(item, 'itunes:episode', ns)
        episode['season'] = get_element_text(item, 'itunes:season', ns)
        episode['explicit'] = get_element_text(item, 'itunes:explicit', ns)

        # Cleanup and convert numeric fields
        episode['duration_seconds'] = parse_duration(episode['duration'])

        if episode['episode_number']:
            try:
                episode['episode_number'] = int(episode['episode_number'])
            except ValueError:
                logger.warning(f"Invalid episode number for {episode['title']}")

        if episode['season']:
            try:
                episode['season'] = int(episode['season'])
            except ValueError:
                logger.warning(f"Invalid season number for {episode['title']}")

        # Additional fields
        episode['guid'] = get_element_text(item, 'guid')

        episodes.append(episode)

    logger.info(f"Extracted {len(episodes)} episodes")
    return episodes


def get_element_text(element, tag, namespaces=None, attr=None):
    """
    Helper function to safely extract text from XML elements

    Args:
        element: Parent XML element
        tag: Tag name to find
        namespaces: Optional namespace dict
        attr: If provided, get this attribute instead of text

    Returns:
        str: Text content or attribute value, or None if not found
    """
    found = element.find(tag, namespaces) if namespaces else element.find(tag)

    if found is not None:
        if attr:
            return found.attrib.get(attr)
        return found.text
    return None


def parse_date(date_str):
    """
    Parse RSS date formats to datetime

    Args:
        date_str: Date string in RSS format

    Returns:
        datetime: Parsed datetime object
    """
    date_formats = [
        '%a, %d %b %Y %H:%M:%S %z',  # RFC 822 format
        '%a, %d %b %Y %H:%M:%S %Z',  # RFC 822 with timezone name
        '%Y-%m-%dT%H:%M:%S%z',  # ISO 8601
        '%Y-%m-%dT%H:%M:%S.%f%z',  # ISO 8601 with microseconds
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    # Try without timezone if all else fails
    try:
        return datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S')
    except ValueError:
        logger.warning(f"Could not parse date: {date_str}")
        return None


def parse_duration(duration_str):
    """
    Parse iTunes duration strings to seconds

    Args:
        duration_str: Duration string (e.g. "1:30:45" or "5400")

    Returns:
        int: Duration in seconds
    """
    if not duration_str:
        return None

    # Check if it's already in seconds
    if duration_str.isdigit():
        return int(duration_str)

    # Handle formats like 1:30:45 (hh:mm:ss) or 30:45 (mm:ss)
    parts = duration_str.split(':')

    if len(parts) == 3:  # hh:mm:ss
        try:
            h, m, s = map(int, parts)
            return h * 3600 + m * 60 + s
        except ValueError:
            logger.warning(f"Invalid duration format: {duration_str}")
    elif len(parts) == 2:  # mm:ss
        try:
            m, s = map(int, parts)
            return m * 60 + s
        except ValueError:
            logger.warning(f"Invalid duration format: {duration_str}")

    return None


def create_episode_dataframe(episodes):
    """
    Convert episode list to DataFrame

    Args:
        episodes: List of episode dictionaries

    Returns:
        DataFrame: Pandas DataFrame of episodes
    """
    df = pd.DataFrame(episodes)

    # Set numeric types
    numeric_cols = ['duration_seconds', 'episode_number', 'season']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Set date types
    if 'pub_date_iso' in df.columns:
        df['pub_date_iso'] = pd.to_datetime(df['pub_date_iso'], errors='coerce')

    return df


def parse_rss_feed(xml_path, output_dir):
    """
    Main function to parse RSS feed

    Args:
        xml_path: Path to RSS XML file
        output_dir: Directory to save output files

    Returns:
        tuple: (podcast_metadata, episodes_df)
    """
    logger.info(f"Parsing RSS feed: {xml_path}")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")

    try:
        # Parse XML
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract data
        podcast_metadata = extract_podcast_metadata(root)
        episodes = extract_episodes(root)

        # Create dataframe
        episodes_df = create_episode_dataframe(episodes)

        # Save results
        metadata_path = os.path.join(output_dir, 'podcast_metadata.json')
        episodes_path = os.path.join(output_dir, 'episodes.csv')

        with open(metadata_path, 'w') as f:
            json.dump(podcast_metadata, f, indent=2)

        episodes_df.to_csv(episodes_path, index=False)

        logger.info(f"Saved podcast metadata to {metadata_path}")
        logger.info(f"Saved {len(episodes_df)} episodes to {episodes_path}")

        return podcast_metadata, episodes_df

    except Exception as e:
        logger.error(f"Error parsing RSS feed: {e}")
        raise


if __name__ == "__main__":
    # For testing/standalone execution
    import argparse

    parser = argparse.ArgumentParser(description='Parse podcast  feed')
    parser.add_argument('--xml_path', help='Path to a XML file')
    parser.add_argument('--output_dir', help='Directory to save output files')

    args = parser.parse_args()

    parse_rss_feed(args.xml_path, args.output_dir)