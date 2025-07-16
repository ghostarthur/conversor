import os
import logging
import tempfile
import subprocess
import re
from datetime import datetime, date
from urllib.parse import urlparse, parse_qs
from flask import Flask, render_template, request, send_file, flash, redirect, url_for, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
import yt_dlp
import threading
import time
from werkzeug.middleware.proxy_fix import ProxyFix

# Configure logging
logging.basicConfig(level=logging.DEBUG)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "youtube-converter-secret-key")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configure database
database_url = os.environ.get("DATABASE_URL")
if database_url:
    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "pool_recycle": 300,
        "pool_pre_ping": True,
    }
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    
    # Initialize database
    db.init_app(app)

# Database Models
class Conversion(db.Model):
    """Model to store YouTube to MP3 conversion history"""
    __tablename__ = 'conversions'
    
    id = db.Column(db.Integer, primary_key=True)
    video_id = db.Column(db.String(20), nullable=False, unique=True, index=True)
    video_title = db.Column(db.String(500), nullable=False)
    video_url = db.Column(db.Text, nullable=False)
    file_size = db.Column(db.Integer)  # Size in bytes
    duration = db.Column(db.Integer)  # Duration in seconds
    quality = db.Column(db.String(10), default='192kbps')
    status = db.Column(db.String(20), default='pending')  # pending, completed, error
    error_message = db.Column(db.Text)
    file_path = db.Column(db.String(500))
    download_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    completed_at = db.Column(db.DateTime)
    last_downloaded = db.Column(db.DateTime)

class DownloadStats(db.Model):
    """Model to store daily download statistics"""
    __tablename__ = 'download_stats'
    
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False, unique=True, index=True)
    total_conversions = db.Column(db.Integer, default=0)
    total_downloads = db.Column(db.Integer, default=0)
    total_file_size = db.Column(db.BigInteger, default=0)  # Total bytes converted
    unique_videos = db.Column(db.Integer, default=0)  # Unique video IDs
    avg_conversion_time = db.Column(db.Float)  # Average time in seconds
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

# Create tables if database is configured
if database_url:
    with app.app_context():
        db.create_all()

# Global variable to store conversion progress
conversion_progress = {}

def is_valid_youtube_url(url):
    """Validate if the URL is a valid YouTube URL"""
    youtube_patterns = [
        r'^https?://(www\.)?youtube\.com/watch\?v=[\w-]+',
        r'^https?://(www\.)?youtu\.be/[\w-]+',
        r'^https?://(www\.)?youtube\.com/embed/[\w-]+',
        r'^https?://(www\.)?youtube\.com/v/[\w-]+',
    ]
    
    for pattern in youtube_patterns:
        if re.match(pattern, url):
            return True
    return False

def extract_video_id(url):
    """Extract video ID from YouTube URL"""
    parsed_url = urlparse(url)
    
    if 'youtube.com' in parsed_url.netloc:
        if 'watch' in parsed_url.path:
            return parse_qs(parsed_url.query).get('v', [None])[0]
        elif 'embed' in parsed_url.path:
            return parsed_url.path.split('/')[-1]
        elif 'v' in parsed_url.path:
            return parsed_url.path.split('/')[-1]
    elif 'youtu.be' in parsed_url.netloc:
        return parsed_url.path[1:]
    
    return None

def progress_hook(d):
    """Progress hook for yt-dlp"""
    if d['status'] == 'downloading':
        percent = d.get('_percent_str', 'N/A')
        speed = d.get('_speed_str', 'N/A')
        video_id = d.get('info_dict', {}).get('id', 'unknown')
        
        conversion_progress[video_id] = {
            'status': 'downloading',
            'percent': percent,
            'speed': speed
        }
        logging.debug(f"Download progress: {percent} at {speed}")
    
    elif d['status'] == 'finished':
        video_id = d.get('info_dict', {}).get('id', 'unknown')
        conversion_progress[video_id] = {
            'status': 'converting',
            'percent': '100%',
            'speed': 'N/A'
        }
        logging.debug("Download finished, starting conversion...")

def update_daily_stats(start_time):
    """Update daily statistics"""
    if not database_url:
        return
    
    try:
        today = date.today()
        stats = DownloadStats.query.filter_by(date=today).first()
        
        if not stats:
            stats = DownloadStats(
                date=today,
                total_conversions=0,
                total_downloads=0,
                total_file_size=0,
                unique_videos=0
            )
            db.session.add(stats)
        
        stats.total_conversions += 1
        conversion_time = (datetime.utcnow() - start_time).total_seconds()
        
        if stats.avg_conversion_time:
            stats.avg_conversion_time = (stats.avg_conversion_time + conversion_time) / 2
        else:
            stats.avg_conversion_time = conversion_time
        
        db.session.commit()
    except Exception as e:
        logging.error(f"Error updating daily stats: {str(e)}")

def download_and_convert(url, output_path, video_id):
    """Download YouTube video and convert to MP3"""
    start_time = datetime.utcnow()
    conversion = None
    
    try:
        # Get video info first
        with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            title = info.get('title', 'Unknown') if info else 'Unknown'
            duration = info.get('duration', 0) if info else 0
        
        # Create or get conversion record if database is available
        if database_url:
            with app.app_context():
                conversion = Conversion.query.filter_by(video_id=video_id).first()
                if not conversion:
                    conversion = Conversion(
                        video_id=video_id,
                        video_title=title,
                        video_url=url,
                        duration=duration,
                        status='starting'
                    )
                    db.session.add(conversion)
                    db.session.commit()
        
        conversion_progress[video_id] = {
            'status': 'starting',
            'percent': '0%',
            'speed': 'N/A'
        }
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(output_path, '%(title)s.%(ext)s'),
            'progress_hooks': [progress_hook],
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'postprocessor_args': [
                '-ar', '44100'
            ],
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Download and convert
            ydl.download([url])
            
            # Find the converted MP3 file
            mp3_file = None
            for file in os.listdir(output_path):
                if file.endswith('.mp3'):
                    mp3_file = os.path.join(output_path, file)
                    break
            
            file_size = 0
            if mp3_file and os.path.exists(mp3_file):
                file_size = os.path.getsize(mp3_file)
            
            # Update conversion record if database is available
            if database_url and conversion:
                with app.app_context():
                    conversion = Conversion.query.filter_by(video_id=video_id).first()
                    if conversion:
                        conversion.status = 'completed'
                        conversion.completed_at = datetime.utcnow()
                        conversion.file_path = mp3_file
                        conversion.file_size = file_size
                        db.session.commit()
                        
                        # Update daily stats
                        update_daily_stats(start_time)
            
            conversion_progress[video_id] = {
                'status': 'completed',
                'percent': '100%',
                'speed': 'N/A',
                'title': title
            }
            
            return True, title
            
    except Exception as e:
        logging.error(f"Error during conversion: {str(e)}")
        
        # Update conversion record with error if database is available
        if database_url and conversion:
            with app.app_context():
                conversion = Conversion.query.filter_by(video_id=video_id).first()
                if conversion:
                    conversion.status = 'error'
                    conversion.error_message = str(e)
                    db.session.commit()
        
        conversion_progress[video_id] = {
            'status': 'error',
            'percent': '0%',
            'speed': 'N/A',
            'error': str(e)
        }
        return False, str(e)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/convert', methods=['POST'])
def convert():
    url = request.form.get('url', '').strip()
    
    if not url:
        flash('Por favor, insira uma URL do YouTube.', 'error')
        return redirect(url_for('index'))
    
    if not is_valid_youtube_url(url):
        flash('URL inválida. Por favor, insira uma URL válida do YouTube.', 'error')
        return redirect(url_for('index'))
    
    video_id = extract_video_id(url)
    if not video_id:
        flash('Não foi possível extrair o ID do vídeo da URL fornecida.', 'error')
        return redirect(url_for('index'))
    
    # Create temporary directory for downloads
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Start conversion in background thread
        thread = threading.Thread(
            target=download_and_convert,
            args=(url, temp_dir, video_id)
        )
        thread.start()
        
        flash('Conversão iniciada! Aguarde...', 'info')
        return render_template('converting.html', video_id=video_id)
        
    except Exception as e:
        logging.error(f"Error starting conversion: {str(e)}")
        flash(f'Erro ao iniciar conversão: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/progress/<video_id>')
def get_progress(video_id):
    """Get conversion progress for a specific video"""
    progress = conversion_progress.get(video_id, {
        'status': 'unknown',
        'percent': '0%',
        'speed': 'N/A'
    })
    return jsonify(progress)

@app.route('/download/<video_id>')
def download_file(video_id):
    """Download the converted MP3 file"""
    progress = conversion_progress.get(video_id, {})
    
    if progress.get('status') != 'completed':
        flash('Conversão ainda não foi concluída.', 'error')
        return redirect(url_for('index'))
    
    # Update download count in database if available
    if database_url:
        try:
            with app.app_context():
                conversion = Conversion.query.filter_by(video_id=video_id).first()
                if conversion:
                    conversion.download_count += 1
                    conversion.last_downloaded = datetime.utcnow()
                    db.session.commit()
                    
                    # Update daily stats
                    today = date.today()
                    stats = DownloadStats.query.filter_by(date=today).first()
                    if stats:
                        stats.total_downloads += 1
                        db.session.commit()
        except Exception as e:
            logging.error(f"Error updating download stats: {str(e)}")
    
    # Find the MP3 file in temporary directories
    # This is a simplified approach - in production, you'd want better file management
    for temp_dir in [f for f in os.listdir('/tmp') if f.startswith('tmp')]:
        temp_path = os.path.join('/tmp', temp_dir)
        if os.path.isdir(temp_path):
            for file in os.listdir(temp_path):
                if file.endswith('.mp3'):
                    file_path = os.path.join(temp_path, file)
                    return send_file(
                        file_path,
                        as_attachment=True,
                        download_name=file,
                        mimetype='audio/mpeg'
                    )
    
    flash('Arquivo não encontrado. Tente converter novamente.', 'error')
    return redirect(url_for('index'))

@app.route('/stats')
def stats():
    """Show conversion statistics"""
    if not database_url:
        flash('Banco de dados não disponível.', 'error')
        return redirect(url_for('index'))
    
    try:
        # Get recent conversions
        recent_conversions = Conversion.query.filter_by(status='completed').order_by(Conversion.completed_at.desc()).limit(10).all()
        
        # Get daily stats for last 7 days
        daily_stats = DownloadStats.query.order_by(DownloadStats.date.desc()).limit(7).all()
        
        # Calculate totals
        total_conversions = Conversion.query.filter_by(status='completed').count()
        total_downloads = sum(conv.download_count for conv in Conversion.query.all())
        
        return render_template('stats.html', 
                             recent_conversions=recent_conversions,
                             daily_stats=daily_stats,
                             total_conversions=total_conversions,
                             total_downloads=total_downloads)
    except Exception as e:
        logging.error(f"Error getting stats: {str(e)}")
        flash('Erro ao carregar estatísticas.', 'error')
        return redirect(url_for('index'))

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    if not database_url:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        # Get basic stats
        total_conversions = Conversion.query.filter_by(status='completed').count()
        total_downloads = sum(conv.download_count for conv in Conversion.query.all())
        
        # Get today's stats
        today = date.today()
        today_stats = DownloadStats.query.filter_by(date=today).first()
        
        return jsonify({
            'total_conversions': total_conversions,
            'total_downloads': total_downloads,
            'today_conversions': today_stats.total_conversions if today_stats else 0,
            'today_downloads': today_stats.total_downloads if today_stats else 0,
            'avg_conversion_time': today_stats.avg_conversion_time if today_stats else 0
        })
    except Exception as e:
        logging.error(f"Error getting API stats: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
