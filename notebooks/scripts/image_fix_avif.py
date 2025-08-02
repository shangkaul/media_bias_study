import json
import imghdr
from pathlib import Path
from PIL import Image
import io
import shutil
import tempfile

# Try to import optional dependencies
AVIF_SUPPORT = False
SVG_SUPPORT = False

try:
    import pillow_avif
    AVIF_SUPPORT = True
except ImportError:
    print("Warning: pillow-avif not available. AVIF files will be converted using Pillow's basic support.")

try:
    from cairosvg import svg2png
    SVG_SUPPORT = True
except ImportError:
    print("Warning: cairosvg not available. SVG files will be skipped.")
    print("Install with: pip install cairosvg")

def is_avif_file(header_bytes):
    """Check if file is AVIF format based on header"""
    return (header_bytes.startswith(b'\x00\x00\x00\x1cftypavif') or 
            header_bytes.startswith(b'\x00\x00\x00\x18ftypavif'))

def is_svg_file(header_bytes):
    """Check if file is SVG format based on header"""
    return (header_bytes.startswith(b'<svg xmlns="') or 
            header_bytes.startswith(b'<?xml versio'))

def fix_image_in_place(file_path):
    """
    Fix image file in place - convert problematic formats to PNG/JPEG
    Keeps the same file path
    """
    # Handle None/null paths
    if file_path is None or file_path == "" or not isinstance(file_path, str):
        print(f"Invalid path: {file_path}")
        return False
        
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return False
    
    try:
        # Read first 20 bytes to identify format
        with open(file_path, 'rb') as f:
            header = f.read(20)
        
        # Handle empty files - skip
        if len(header) == 0:
            print(f"Empty file, skipping: {file_path}")
            return False
        
        # Create backup in temp file
        with tempfile.NamedTemporaryFile(delete=False) as temp_backup:
            shutil.copy2(file_path, temp_backup.name)
            backup_path = temp_backup.name
        
        try:
            # Check if we actually need to convert this file
            needs_conversion = False
            
            # Handle SVG files
            if is_svg_file(header):
                if not SVG_SUPPORT:
                    print(f"SVG file but no cairosvg support, skipping: {file_path}")
                    Path(backup_path).unlink()
                    return False
                    
                print(f"Converting SVG to PNG: {file_path}")
                with open(file_path, 'rb') as f:
                    svg_data = f.read()
                
                # Convert SVG to PNG
                png_data = svg2png(bytestring=svg_data, output_width=512, output_height=512)
                image = Image.open(io.BytesIO(png_data)).convert('RGB')
                # Save as PNG to preserve transparency if needed
                image.save(file_path, 'PNG')
                print(f"Successfully converted SVG to PNG: {file_path}")
                Path(backup_path).unlink()
                return True
                
            # Handle AVIF files specifically
            elif is_avif_file(header):
                # Determine output format based on file extension
                file_ext = file_path.suffix.lower()
                
                if file_ext in ['.png']:
                    print(f"Converting AVIF to PNG (preserving .png extension): {file_path}")
                    image = Image.open(file_path)
                    if image.mode not in ['RGB', 'RGBA']:
                        image = image.convert('RGBA')  # PNG supports transparency
                    image.save(file_path, 'PNG')
                    print(f"Successfully converted AVIF to PNG: {file_path}")
                    
                elif file_ext in ['.jpg', '.jpeg']:
                    print(f"Converting AVIF to JPEG (preserving .jpg extension): {file_path}")
                    image = Image.open(file_path)
                    if image.mode != 'RGB':
                        image = image.convert('RGB')
                    image.save(file_path, 'JPEG', quality=95)
                    print(f"Successfully converted AVIF to JPEG: {file_path}")
                    
                elif file_ext in ['.webp']:
                    print(f"Converting AVIF to WebP (preserving .webp extension): {file_path}")
                    image = Image.open(file_path)
                    if image.mode not in ['RGB', 'RGBA']:
                        image = image.convert('RGBA')
                    image.save(file_path, 'WebP', quality=95)
                    print(f"Successfully converted AVIF to WebP: {file_path}")
                    
                else:
                    # Default to JPEG for unknown extensions
                    print(f"Converting AVIF to JPEG (unknown extension {file_ext}): {file_path}")
                    image = Image.open(file_path)
                    if image.mode != 'RGB':
                        image = image.convert('RGB')
                    image.save(file_path, 'JPEG', quality=95)
                    print(f"Successfully converted AVIF to JPEG: {file_path}")
                
                Path(backup_path).unlink()
                return True
                
            else:
                # For other formats, try to load and see if PIL can handle it now
                try:
                    image = Image.open(file_path)
                    # If we can load it successfully, no conversion needed
                    print(f"File is now readable, no conversion needed: {file_path}")
                    Path(backup_path).unlink()
                    return True
                except Exception:
                    print(f"File still unreadable, skipping: {file_path}")
                    Path(backup_path).unlink()
                    return False
                
        except Exception as e:
            print(f"Failed to convert {file_path}: {e}")
            # Restore from backup
            shutil.copy2(backup_path, file_path)
            Path(backup_path).unlink()
            return False
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def fix_all_unreadable_images(unreadable_json_path="unreadable_images.json"):
    """
    Fix all unreadable images in place
    """
    # Load the bad entries
    with open(unreadable_json_path, "r") as f:
        bad_entries = json.load(f)
    
    # Extract paths, filtering out None/null/empty values
    image_paths = []
    null_count = 0
    
    for entry in bad_entries:
        path = entry.get("path")
        if path is None or path == "" or not isinstance(path, str):
            null_count += 1
            continue
        image_paths.append(path)
    
    if null_count > 0:
        print(f"Skipped {null_count} entries with null/empty paths")
    
    print(f"Fixing {len(image_paths)} unreadable images in place...")
    
    success_count = 0
    failed_paths = []
    
    for i, path in enumerate(image_paths):
        if i % 1000 == 0:
            print(f"Progress: {i}/{len(image_paths)}")
            
        if fix_image_in_place(path):
            success_count += 1
        else:
            failed_paths.append(path)
    
    print(f"\nResults:")
    print(f"Successfully fixed: {success_count} images")
    print(f"Failed to fix: {len(failed_paths)} images")
    
    # Save the still-failed ones
    if failed_paths:
        with open("still_failed_after_fix.json", "w") as f:
            json.dump([{"path": path} for path in failed_paths], f, indent=2)
        print("Saved still-failed images to 'still_failed_after_fix.json'")
    
    return success_count, failed_paths

if __name__ == "__main__":
    # Install SVG support if needed: pip install cairosvg
    # AVIF support may work with newer Pillow versions (10.0.0+)
    
    print(f"AVIF support: {'Available' if AVIF_SUPPORT else 'Not available (using basic Pillow)'}")
    print(f"SVG support: {'Available' if SVG_SUPPORT else 'Not available'}")
    
    # Fix all unreadable images in place
    success_count, failed_paths = fix_all_unreadable_images()
    
    print(f"\nFixed {success_count} images in place!")
    print("All file paths remain the same - your pipeline should work now.")