import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm


def extract_frames(video_path, output_root):
    video_name = os.path.splitext(os.path.basename(video_path))[0]
    output_dir = os.path.join(output_root, video_name)
    os.makedirs(output_dir, exist_ok=True)

    # Lossless extraction using .png
    # ffmpeg default for PNG is lossless (compression_level 100)
    command = [
        'ffmpeg',
        '-i', video_path,
        '-q:v', '2',
        os.path.join(output_dir, '%04d.jpg'),
        '-hide_banner',
        '-loglevel', 'error'  # Keeps the console clean
    ]

    subprocess.run(command)
    return video_name


def process_all_videos(input_folder, output_folder, max_workers=os.cpu_count()):
    # List all mp4 files
    videos = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.mp4')]

    if not videos:
        print("No .mp4 files found in the input directory.")
        return

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and store them in a list
        futures = [executor.submit(extract_frames, v, output_folder) for v in videos]

        # Wrap with tqdm progress bar
        # total=len(videos) sets the 100% mark
        for _ in tqdm(as_completed(futures), total=len(videos), desc="Extracting Frames", unit="video"):
            pass


if __name__ == "__main__":
    # --- CONFIGURATION ---
    input_dir = 'video_frames_data/videos'
    output_dir = 'video_frames_data/frames'

    process_all_videos(input_dir, output_dir)
