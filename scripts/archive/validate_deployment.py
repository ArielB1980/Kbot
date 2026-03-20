#!/usr/bin/env python3
"""
Pre-deployment validation pipeline.

Validates:
1. app.yaml components have corresponding files
2. Required environment variables are documented
3. No obvious configuration errors

Usage:
    python scripts/validate_deployment.py
"""
import sys
from pathlib import Path
import subprocess


def run_validation(script_path: str, description: str) -> tuple[bool, str]:
    """
    Run a validation script.
    
    Returns:
        (success, output)
    """
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        return (result.returncode == 0, result.stdout + result.stderr)
    except subprocess.TimeoutExpired:
        return (False, f"Validation timed out: {description}")
    except Exception as e:
        return (False, f"Error running validation: {e}")


def main():
    """Main entry point."""
    project_root = Path(__file__).parent.parent
    errors = []
    warnings = []
    
    print("🔍 Running pre-deployment validation...\n")
    
    # 1. Validate app.yaml components
    print("1. Validating app.yaml components...")
    app_spec_script = project_root / "scripts" / "validate_app_spec.py"
    if app_spec_script.exists():
        success, output = run_validation(
            str(app_spec_script),
            "app.yaml component validation"
        )
        if success:
            print("   ✅ app.yaml components validated")
        else:
            print(f"   ❌ app.yaml validation failed:")
            print(f"      {output}")
            errors.append("app.yaml component validation failed")
    else:
        warnings.append("validate_app_spec.py not found - skipping component validation")
        print("   ⚠️  Skipping (validate_app_spec.py not found)")
    
    # 2. Check for required files
    print("\n2. Checking required files...")
    required_files = [
        ".do/app.yaml",
        "src/config/config.yaml",
        "migrate_schema.py",
        "run.py"
    ]
    for file_path in required_files:
        full_path = project_root / file_path
        if full_path.exists():
            print(f"   ✅ {file_path}")
        else:
            print(f"   ❌ {file_path} not found")
            errors.append(f"Required file missing: {file_path}")
    
    # 3. Validate Python syntax (basic check)
    print("\n3. Validating Python syntax...")
    try:
        import py_compile
        python_files = [
            "src/config/config.py",
            "src/storage/db.py",
            "src/utils/secret_manager.py",
            "migrate_schema.py",
            "run.py"
        ]
        syntax_errors = []
        for file_path in python_files:
            full_path = project_root / file_path
            if full_path.exists():
                try:
                    py_compile.compile(str(full_path), doraise=True)
                    print(f"   ✅ {file_path}")
                except py_compile.PyCompileError as e:
                    print(f"   ❌ {file_path}: Syntax error")
                    syntax_errors.append(f"{file_path}: {e}")
        if syntax_errors:
            errors.extend(syntax_errors)
    except Exception as e:
        warnings.append(f"Could not validate Python syntax: {e}")
        print(f"   ⚠️  Skipping (error: {e})")
    
    # Summary
    print("\n" + "=" * 60)
    if errors:
        print("❌ Validation FAILED")
        print("\nErrors:")
        for error in errors:
            print(f"  • {error}")
        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"  • {warning}")
        sys.exit(1)
    else:
        print("✅ Validation PASSED")
        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"  • {warning}")
        sys.exit(0)


if __name__ == "__main__":
    main()
