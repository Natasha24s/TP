
import pandas as pd
import numpy as np
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Color
import os
import warnings

# Suppress specific warnings
warnings.filterwarnings('ignore', category=FutureWarning)

def get_service_domains():
    """Return list of service domains and their associated services"""
    return {
        'Analytics Services': [
            'EMR', 'Athena', 'Analytics', 'Glue', 'QuickSight', 'Redshift',
            'Kinesis Analytics', 'Lake Formation', 'Data Pipeline', 'OpenSearch'
        ],
        'Automation and Messaging Group': [
            'SNS', 'SQS', 'EventBridge', 'MQ', 'Step Functions', 'Simple Queue Service',
            'Simple Notification Service', 'Amazon MQ', 'AWS Step Functions'
        ],
        'Compute Services': [
            'EC2', 'ECS', 'EKS', 'Fargate', 'Lambda', 'Compute', 'Container',
            'NAT Gateway', 'Elastic Load Balancing', 'Load Balancer', 'Elastic Compute',
            'Elastic Container', 'Elastic Kubernetes', 'AWS Lambda', 'Savings Plan'
        ],
        'DB Services': [
            'RDS', 'DynamoDB', 'Elasticache', 'Aurora', 'Database', 'DDB',
            'MySQL', 'PostgreSQL', 'MariaDB', 'SQL Server', 'Neptune', 'DocumentDB',
            'Relational Database', 'NoSQL', 'Redis', 'Memcached'
        ],
        'Developer Tools': [
            'CodeBuild', 'CodePipeline', 'CodeDeploy', 'CodeCommit', 'Cloud9',
            'CodeStar', 'CodeArtifact', 'CodeGuru', 'X-Ray', 'Developer Tools'
        ],
        'Edge': [
            'CloudFront', 'Route53', 'PerimeterProtection', 'Edge', 'CDN',
            'DNS', 'Cloud Front', 'Global Accelerator', 'Route 53'
        ],
        'Identity Services': [
            'IAM', 'Directory Service', 'Cognito', 'SSO', 'Single Sign-On',
            'Identity Center', 'Resource Access Manager', 'AWS Organizations'
        ],
        'Machine Learning & Deep Learning': [
            'SageMaker', 'Rekognition', 'Comprehend', 'ML', 'AI', 'Deep Learning',
            'Forecast', 'Textract', 'Polly', 'Transcribe', 'Bedrock', 'Lex',
            'Personalize', 'Translate', 'DeepLens', 'DeepRacer'
        ],
        'Management Tools': [
            'CloudTrail', 'Config', 'SSM', 'Management', 'Systems Manager',
            'OpsWorks', 'Service Catalog', 'Control Tower', 'License Manager',
            'Managed Services', 'CloudFormation', 'Auto Scaling'
        ],
        'Marketplaces': [
            'AWS Marketplace', 'Marketplace Subscriptions', 'Marketplace Entitlements',
            'Developer Marketplace'
        ],
        'Marketplaces/Control Services': [
            'AWS Control Services', 'Service Catalog', 'Control Tower',
            'AWS Organizations', 'AWS Control Tower'
        ],
        'Migration-Services': [
            'Migration', 'Transfer', 'DataSync', 'Application Discovery',
            'Database Migration', 'Server Migration', 'Migration Hub'
        ],
        'Mobile Services': [
            'Mobile Hub', 'AppSync', 'Device Farm', 'Amplify', 'API Gateway',
            'Mobile Analytics'
        ],
        'Monitoring Services': [
            'CloudWatch', 'Monitoring', 'Logs', 'Metrics', 'Events',
            'Performance', 'Application Insights', 'Synthetics'
        ],
        'Networking Bandwidth Services': [
            'Data Transfer', 'DTO', 'DTIR', 'Bandwidth', 'Network',
            'PrivateLink', 'Transit Gateway', 'VPC', 'Direct Connect',
            'Virtual Private Network', 'Elastic Load Balancing'
        ],
        'Others': [
            'Other', 'Miscellaneous', 'Additional Services'
        ],
        'Productivity Applications': [
            'WorkSpaces', 'WorkDocs', 'Chime', 'Connect', 'Communication',
            'Productivity', 'Workmail', 'AppStream', 'WorkLink'
        ],
        'Professional Services/Training': [
            'Professional Services', 'Training', 'Support', 'Consulting',
            'AWS Training', 'Technical Support', 'Business Support',
            'Enterprise Support', 'AWS Certification'
        ],
        'Security Services': [
            'GuardDuty', 'KMS', 'Security', 'Inspector', 'IAM', 'Certificate',
            'Firewall', 'WAF', 'Shield', 'Secrets Manager', 'ESS', 'Macie',
            'Security Hub', 'Detective', 'Network Firewall', 'ACM'
        ],
        'Storage': [
            'S3', 'EBS', 'EFS', 'Storage', 'Glacier', 'Elastic Block Store',
            'Backup', 'Transfer', 'FSx', 'Storage Gateway', 'Simple Storage Service'
        ],
        'Streaming Services': [
            'Kinesis', 'SNS', 'SQS', 'EventBridge', 'MQ', 'Streaming',
            'Message Queue', 'Notification', 'Data Streams', 'Firehose',
            'Video Streams', 'Amazon MSK'
        ],
        'Support Services': [
            'AWS Support', 'Premium Support', 'Basic Support', 'Developer Support',
            'Business Support', 'Enterprise Support'
        ],
        'Uncategorized': []
    }


def clean_excel_data(excel_file_path):
    """Clean and standardize Excel data"""
    excel_file = pd.ExcelFile(excel_file_path)
    cleaned_sheets = {}
    
    for sheet_name in excel_file.sheet_names:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            if df.empty or df.dropna().empty:
                print(f"Sheet '{sheet_name}' is empty - skipping")
                continue
            
            print(f"\nProcessing sheet: {sheet_name}")
            print(f"Original shape: {df.shape}")
            
            new_columns = []
            for col in df.columns:
                if isinstance(col, str):
                    if 'Service group' in col:
                        new_columns.append('Service group')
                    elif '12 months total' in col:
                        new_columns.append('Last 12 months total')
                    else:
                        new_columns.append(col)
                elif isinstance(col, datetime):
                    new_columns.append(col.strftime('%b-%y'))
                else:
                    new_columns.append(col)
            
            df.columns = new_columns
            df = df.replace('-', 0)
            
            if not df.empty:
                cleaned_sheets[sheet_name] = df
                print(f"Shape after cleaning: {df.shape}")
                print("\nColumn names after cleaning:")
                print(df.columns.tolist())
            
        except Exception as e:
            print(f"Error processing sheet '{sheet_name}': {str(e)}")
            continue
    
    return cleaned_sheets

def clean_amount(value):
    """Clean and convert monetary values to float"""
    try:
        if pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            if '(free tier)' in value:
                return 0.0
            cleaned = value.replace('US$', '').replace('$', '').replace(',', '').replace(' ', '')
            if cleaned == '-' or cleaned == '':
                return 0.0
            return float(cleaned)
        return 0.0
    except Exception as e:
        print(f"Warning: Could not convert '{value}' to float. Using 0.0 instead.")
        return 0.0

def get_service_domain(service_name, domain_mapping):
    """Match a service name to its domain"""
    service_name = str(service_name).lower()
    for domain, services in domain_mapping.items():
        if any(service.lower() in service_name for service in services):
            return domain
    return None

def get_account_total_spend(df):
    """Get total spend for an account from the first data row"""
    try:
        # Skip empty rows and headers
        for idx, row in df.iterrows():
            # Find the first non-empty row after headers
            if pd.notna(row.iloc[0]) and isinstance(row.iloc[0], str):
                if not row.iloc[0].startswith('Service group'):
                    # Get value from "Last 12 months total" column
                    total_spend_str = row.iloc[1]  # Column index 1 contains "Last 12 months total"
                    # Clean and convert the value
                    if isinstance(total_spend_str, str):
                        total_spend = float(total_spend_str.replace('US$', '').replace(',', ''))
                    else:
                        total_spend = float(total_spend_str)
                    account_name = row.iloc[0]
                    print(f"Found total spend for {account_name}: ${total_spend:,.2f}")
                    return total_spend
        return 0.0
    except Exception as e:
        print(f"Error getting account total spend: {str(e)}")
        return 0.0

def analyze_service_domains(cleaned_data):
    """Analyze spending by service domain"""
    domain_mapping = get_service_domains()
    domain_totals = {domain: 0 for domain in domain_mapping.keys()}
    domain_accounts = {domain: set() for domain in domain_mapping.keys()}
    account_domain_spend = {}
    account_totals = {}
    
    # First pass - get account totals
    for sheet_name, df in cleaned_data.items():
        account_totals[sheet_name] = get_account_total_spend(df)
    
    # Store account totals in the account_domain_spend dictionary
    for sheet_name in cleaned_data.keys():
        account_domain_spend[sheet_name] = {
            'total': account_totals[sheet_name],
            'domains': {domain: 0 for domain in domain_mapping.keys()}
        }
    
    # Second pass - process services
    for sheet_name, df in cleaned_data.items():
        try:
            for idx, row in df.iterrows():
                service_name = str(row.iloc[0])
                if pd.isna(service_name) or 'View AWS service' in service_name:
                    continue
                
                domain = get_service_domain(service_name, domain_mapping)
                if domain:
                    spend = clean_amount(row.iloc[1])
                    if spend > 0:
                        domain_totals[domain] += spend
                        domain_accounts[domain].add(sheet_name)
                        account_domain_spend[sheet_name]['domains'][domain] += spend
        
        except Exception as e:
            print(f"Error processing {sheet_name}: {str(e)}")
    
    # Create summary using actual account totals
    summary_data = []
    total_spend = sum(account_totals.values())
    
    for domain in domain_mapping.keys():
        spend = domain_totals[domain]
        unique_accounts = len(domain_accounts[domain])
        avg_spend = spend / unique_accounts if unique_accounts > 0 else 0
        percentage = (spend / total_spend * 100) if total_spend > 0 else 0
        
        summary_data.append({
            'Service Domain': domain,
            'Total Spend': f"${spend:,.2f}",
            'Number of Accounts': unique_accounts,
            'Average Spend': f"${avg_spend:,.2f}",
            'Percentage of Total': f"{percentage:.2f}%",
            'Active Accounts': ', '.join(sorted(domain_accounts[domain])) if unique_accounts > 0 else 'None'
        })
    
    df_summary = pd.DataFrame(summary_data)
    return df_summary.sort_values(
        'Total Spend',
        key=lambda x: pd.to_numeric(x.str.replace('$', '').replace(',', ''), errors='coerce'),
        ascending=False
    ), account_domain_spend

def create_comparative_analysis(cleaned_data, account_domain_spend):
    """Create comparative analysis between accounts"""
    domain_mapping = get_service_domains()
    comparative_data = []
    monthly_trends = {}
    
    for sheet_name, df in cleaned_data.items():
        try:
            total_spend = account_domain_spend[sheet_name]['total']  # Use stored total
            monthly_spends = []
            
            # Calculate monthly spends
            for month in range(2, 14):
                monthly_total = 0
                for idx, row in df.iterrows():
                    service_name = str(row.iloc[0])
                    if get_service_domain(service_name, domain_mapping):
                        monthly_total += clean_amount(row.iloc[month])
                monthly_spends.append(monthly_total)
            
            # Get domain spends
            domain_spends = account_domain_spend[sheet_name]['domains']
            if domain_spends:
                max_domain = max(domain_spends.items(), key=lambda x: x[1])
                domain_concentration = (max_domain[1] / total_spend * 100) if total_spend > 0 else 0
            else:
                max_domain = ('None', 0)
                domain_concentration = 0
            
            # Calculate month-over-month changes
            mom_changes = []
            for i in range(1, len(monthly_spends)):
                if monthly_spends[i-1] != 0:
                    change = ((monthly_spends[i] - monthly_spends[i-1]) / monthly_spends[i-1]) * 100
                else:
                    change = 0
                mom_changes.append(change)
            
            comparative_data.append({
                'Account': sheet_name,
                'Total Annual Spend': total_spend,
                'Avg Monthly Spend': total_spend / 12 if total_spend > 0 else 0,
                'Highest Spend Domain': max_domain[0],
                'Domain Spend': max_domain[1],
                'Domain Concentration %': domain_concentration,
                'Max Monthly Spend': max(monthly_spends) if monthly_spends else 0,
                'Min Monthly Spend': min(monthly_spends) if monthly_spends else 0,
                'Spend Volatility': np.std(monthly_spends) if monthly_spends else 0,
                'Avg MoM Change %': np.mean(mom_changes) if mom_changes else 0
            })
            
            monthly_trends[sheet_name] = monthly_spends
            
        except Exception as e:
            print(f"Error processing {sheet_name} in comparative analysis: {str(e)}")
            continue
    
    df_comparative = pd.DataFrame(comparative_data)
    
    # Format columns
    for col in df_comparative.columns:
        if col.endswith('Spend'):
            df_comparative[col] = df_comparative[col].apply(lambda x: f"${x:,.2f}")
        elif col.endswith('%'):
            df_comparative[col] = df_comparative[col].apply(lambda x: f"{x:.2f}%")
    
    # Create monthly trends DataFrame
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    df_trends = pd.DataFrame(monthly_trends).T
    df_trends.columns = month_labels
    
    for col in df_trends.columns:
        df_trends[col] = df_trends[col].apply(lambda x: f"${x:,.2f}")
    
    return df_comparative, df_trends
   

def create_cumulative_report(cleaned_data, account_domain_spend):
    """Create detailed cumulative analysis report"""
    domain_mapping = get_service_domains()
    domains = list(domain_mapping.keys())
    cumulative_data = []
    
    # Calculate overall totals using stored account totals
    total_spend = sum(data['total'] for data in account_domain_spend.values())
    num_accounts = len(account_domain_spend)
    
    # Create summary row
    summary_row = {
        'Account': 'TOTAL ALL ACCOUNTS',
        'Total Annual Spend': total_spend,
        'Average Monthly Spend': total_spend / 12 if total_spend > 0 else 0,
        'Number of Active Domains': 0,
        'Most Used Domain': '',
        'Domain Distribution Score': 0
    }
    
    # Calculate domain totals
    domain_totals = {domain: 0 for domain in domains}
    for account_data in account_domain_spend.values():
        for domain, spend in account_data['domains'].items():
            domain_totals[domain] += spend
    
    # Add domain-specific fields to summary
    for domain in domains:
        summary_row[f'{domain} Total'] = domain_totals[domain]
        summary_row[f'{domain} Avg'] = domain_totals[domain] / num_accounts if num_accounts > 0 else 0
    
    # Find most used domain for summary
    if domain_totals:
        most_used = max(domain_totals.items(), key=lambda x: x[1])
        summary_row['Most Used Domain'] = f"{most_used[0]} (${most_used[1]:,.2f})"
        summary_row['Number of Active Domains'] = sum(1 for v in domain_totals.values() if v > 0)
    
    cumulative_data.append(summary_row)
    
    # Process each account
    for account, data in account_domain_spend.items():
        account_total = data['total']
        domain_spends = data['domains']
        active_domains = sum(1 for v in domain_spends.values() if v > 0)
        
        # Calculate domain distribution score
        if account_total > 0:
            proportions = [spend/account_total for spend in domain_spends.values() if spend > 0]
            distribution_score = 1 - np.std(proportions) if proportions else 0
        else:
            distribution_score = 0
        
        account_row = {
            'Account': account,
            'Total Annual Spend': account_total,
            'Average Monthly Spend': account_total / 12,
            'Number of Active Domains': active_domains,
            'Most Used Domain': '',
            'Domain Distribution Score': distribution_score * 100
        }
        
        # Find account's most used domain
        if domain_spends:
            most_used = max(domain_spends.items(), key=lambda x: x[1])
            account_row['Most Used Domain'] = f"{most_used[0]} (${most_used[1]:,.2f})"
        
        # Add domain-specific spending and comparisons
        for domain in domains:
            spend = domain_spends.get(domain, 0)
            avg_spend = summary_row[f'{domain} Avg']
            account_row[f'{domain} Spend'] = spend
            account_row[f'{domain} vs Avg'] = ((spend - avg_spend) / avg_spend * 100) if avg_spend > 0 else 0
        
        cumulative_data.append(account_row)
    
    df_cumulative = pd.DataFrame(cumulative_data)
    
    # Format the DataFrame
    currency_cols = ['Total Annual Spend', 'Average Monthly Spend'] + \
                   [f'{domain} Spend' for domain in domains]
    percentage_cols = [f'{domain} vs Avg' for domain in domains] + ['Domain Distribution Score']
    
    for col in currency_cols:
        if col in df_cumulative.columns:
            df_cumulative[col] = df_cumulative[col].apply(lambda x: f"${x:,.2f}")
    
    for col in percentage_cols:
        if col in df_cumulative.columns:
            df_cumulative[col] = df_cumulative[col].apply(lambda x: f"{x:.1f}%" if isinstance(x, (int, float)) else x)
    
    return df_cumulative

def calculate_category_distribution_score(row, categories):
    """Calculate the distribution score for service categories"""
    percentages = []
    for category in categories:
        try:
            if f"{category} %" in row:
                percentage = float(str(row[f"{category} %"]).replace('%', ''))
                if percentage > 0:
                    percentages.append(percentage)
        except:
            continue
    
    if percentages:
        return round(100 - np.std(percentages), 2)
    return 0.0


def clean_amount_string(val):
    """Helper function to clean and convert amount strings to float"""
    try:
        if pd.isna(val):
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        val_str = str(val).lower()
        if '(free tier)' in val_str:
            return 0.0
        # Remove currency symbols, commas and spaces
        cleaned = val_str.replace('us$', '').replace('$', '').replace(',', '').replace(' ', '')
        if cleaned == '-' or cleaned == '':
            return 0.0
        return float(cleaned)
    except:
        print(f"Warning: Could not convert '{val}' to float. Using 0.0")
        return 0.0

def analyze_monthly_patterns(cleaned_data, account_domain_spend):
    """Analyze monthly spending patterns and detect anomalies"""
    domain_mapping = get_service_domains()
    monthly_patterns = []
    
    for sheet_name, df in cleaned_data.items():
        print(f"\nAnalyzing patterns for {sheet_name}...")
        account_patterns = {
            'Account': sheet_name,
            'Service Patterns': {}
        }
        
        # Get total spend from 'Last 12 months total'
        total_row = df[df.iloc[:, 0].str.contains('total', case=False, na=False)]
        if not total_row.empty:
            total_spend = clean_amount_string(total_row.iloc[0]['Last 12 months total'])
        else:
            continue

        for domain in domain_mapping.keys():
            domain_rows = df[df.apply(lambda row: get_service_domain(str(row.iloc[0]), domain_mapping) == domain, axis=1)]
            if domain_rows.empty:
                continue

            # Get monthly spend for the domain
            monthly_cols = [col for col in df.columns if any(month in col for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])]
            monthly_spend = []
            
            for month in monthly_cols:
                month_total = sum(clean_amount_string(val) for val in domain_rows[month])
                monthly_spend.append(month_total)
            
            if sum(monthly_spend) > 0:
                mean_spend = np.mean(monthly_spend)
                std_spend = np.std(monthly_spend)
                
                anomalies = []
                for month_idx, spend in enumerate(monthly_spend):
                    z_score = (spend - mean_spend) / std_spend if std_spend > 0 else 0
                    if abs(z_score) > 2:
                        anomalies.append({
                            'Month': month_idx + 1,
                            'Spend': spend,
                            'Z-score': z_score
                        })
                
                pattern_info = {
                    'Monthly Spend': monthly_spend,
                    'Average': mean_spend,
                    'Std Dev': std_spend,
                    'Anomalies': anomalies,
                    'Trend': calculate_trend_pattern(monthly_spend),
                    'Max Month': np.argmax(monthly_spend) + 1,
                    'Min Month': np.argmin(monthly_spend) + 1,
                    'Volatility': std_spend / mean_spend if mean_spend > 0 else 0
                }
                
                account_patterns['Service Patterns'][domain] = pattern_info
        
        monthly_patterns.append(account_patterns)
    
    return monthly_patterns

def calculate_trend_pattern(monthly_data):
    """Identify the trend pattern in monthly spending"""
    if not monthly_data or len(monthly_data) < 2:
        return {
            'type': 'insufficient_data',
            'description': 'Insufficient data',
            'trend_slope': 0,
            'coefficient_of_variation': 0,
            'half_year_change': 0
        }
    
    try:
        mean_spend = np.mean(monthly_data)
        std_spend = np.std(monthly_data)
        cv = std_spend / mean_spend if mean_spend > 0 else 0
        
        x = np.arange(len(monthly_data))
        slope, _ = np.polyfit(x, monthly_data, 1)
        
        first_half_avg = np.mean(monthly_data[:6])
        second_half_avg = np.mean(monthly_data[6:])
        half_year_change = ((second_half_avg - first_half_avg) / first_half_avg * 100) if first_half_avg > 0 else 0
        
        if cv > 0.5:
            pattern_type = 'highly_variable'
            description = f'Highly variable spending (CV: {cv:.2f})'
        elif cv > 0.25:
            pattern_type = 'moderately_variable'
            description = f'Moderately variable spending (CV: {cv:.2f})'
        elif slope > 0:
            if half_year_change > 50:
                pattern_type = 'sharp_increase'
                description = f'Sharp increasing trend (+{half_year_change:.1f}% in 6 months)'
            else:
                pattern_type = 'gradual_increase'
                description = f'Gradual increasing trend (+{half_year_change:.1f}% in 6 months)'
        elif slope < 0:
            if half_year_change < -50:
                pattern_type = 'sharp_decrease'
                description = f'Sharp decreasing trend ({half_year_change:.1f}% in 6 months)'
            else:
                pattern_type = 'gradual_decrease'
                description = f'Gradual decreasing trend ({half_year_change:.1f}% in 6 months)'
        else:
            pattern_type = 'stable'
            description = 'Stable spending pattern'
        
        return {
            'type': pattern_type,
            'description': description,
            'coefficient_of_variation': cv,
            'trend_slope': slope,
            'half_year_change': half_year_change
        }
    
    except Exception as e:
        print(f"Error in trend calculation: {str(e)}")
        return {
            'type': 'error',
            'description': 'Error in calculation',
            'trend_slope': 0,
            'coefficient_of_variation': 0,
            'half_year_change': 0
        }

def create_monthly_trends_report(monthly_patterns):
    """Create detailed monthly trends report"""
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    trend_data = []
    
    if not monthly_patterns:
        print("No monthly patterns data available")
        return pd.DataFrame()
    
    for pattern in monthly_patterns:
        try:
            account = pattern['Account']
            
            for domain, info in pattern['Service Patterns'].items():
                monthly_spend = info['Monthly Spend']
                anomalies = info['Anomalies']
                trend = info['Trend']
                
                row_data = {
                    'Account': account,
                    'Service Domain': domain,
                    'Spending Pattern': trend['description'],
                    'Risk Level': calculate_risk_level(info),
                    'Volatility': f"{info['Volatility']*100:.1f}%",
                    'Notable Changes': format_anomalies(anomalies, month_labels),
                    'Average Monthly': f"${info['Average']:,.2f}",
                    'Trend Direction': 'Increasing' if trend.get('trend_slope', 0) > 0 else 'Decreasing'
                }
                
                for month_idx, spend in enumerate(monthly_spend):
                    row_data[month_labels[month_idx]] = f"${spend:,.2f}"
                
                trend_data.append(row_data)
                
        except Exception as e:
            print(f"Error processing trend data for {account}: {str(e)}")
            continue
    
    return pd.DataFrame(trend_data)

def format_anomalies(anomalies, month_labels):
    """Format anomaly information into readable text"""
    if not anomalies:
        return "No significant anomalies"
    
    anomaly_texts = []
    for anomaly in anomalies:
        try:
            month = month_labels[anomaly['Month']-1]
            direction = "spike" if anomaly['Z-score'] > 0 else "drop"
            magnitude = abs(anomaly['Z-score'])
            
            if magnitude > 3:
                severity = "extreme"
            elif magnitude > 2.5:
                severity = "significant"
            else:
                severity = "notable"
            
            amount = f"${anomaly['Spend']:,.2f}"
            anomaly_texts.append(f"{severity.title()} {direction} in {month} ({amount})")
        except Exception as e:
            print(f"Error formatting anomaly: {str(e)}")
            continue
    
    return "; ".join(anomaly_texts) if anomaly_texts else "Error processing anomalies"

def calculate_risk_level(pattern_info):
    """Calculate risk level based on spending pattern"""
    try:
        risk_score = 0
        
        # Factor 1: Volatility
        volatility = pattern_info.get('Volatility', 0)
        if volatility > 0.5:
            risk_score += 3
        elif volatility > 0.25:
            risk_score += 2
        elif volatility > 0.1:
            risk_score += 1
        
        # Factor 2: Number of anomalies
        num_anomalies = len(pattern_info.get('Anomalies', []))
        risk_score += min(num_anomalies, 3)
        
        # Factor 3: Trend pattern
        trend = pattern_info.get('Trend', {})
        if trend.get('type') in ['sharp_increase', 'sharp_decrease', 'highly_variable']:
            risk_score += 2
        elif trend.get('type') in ['gradual_increase', 'gradual_decrease', 'moderately_variable']:
            risk_score += 1
        
        # Determine risk level
        if risk_score >= 6:
            return 'High'
        elif risk_score >= 3:
            return 'Medium'
        return 'Low'
    
    except Exception as e:
        print(f"Error calculating risk level: {str(e)}")
        return 'Unknown'

def format_excel_with_highlights(worksheet, sheet_name=''):
    """Format Excel worksheet with conditional highlighting"""
    # Define colors
    header_color = "1F4E78"
    positive_var = "90EE90"
    negative_var = "FFB6C1"
    risk_high = "FF9999"
    risk_medium = "FFD699"
    summary_row = "E6E6E6"
    alt_row = "F5F5F5"
    
    # Define styles
    header_style = {
        'fill': PatternFill(start_color=header_color, end_color=header_color, fill_type="solid"),
        'font': Font(color="FFFFFF", bold=True),
        'alignment': Alignment(horizontal='center', vertical='center', wrap_text=True)
    }
    
    summary_style = {
        'fill': PatternFill(start_color=summary_row, end_color=summary_row, fill_type="solid"),
        'font': Font(bold=True),
        'alignment': Alignment(horizontal='center', vertical='center', wrap_text=True)
    }
    
    # Apply header formatting
    for cell in worksheet[1]:
        cell.fill = header_style['fill']
        cell.font = header_style['font']
        cell.alignment = header_style['alignment']
    
    # Format data rows
    for row_idx, row in enumerate(worksheet.iter_rows(min_row=2), 2):
        for cell in row:
            # Default alignment
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            
            # Summary row formatting
            if row_idx == 2:
                cell.fill = summary_style['fill']
                cell.font = summary_style['font']
            
            # Alternate row colors
            elif row_idx % 2 == 0:
                cell.fill = PatternFill(start_color=alt_row, end_color=alt_row, fill_type="solid")
            
            # Risk level highlighting
            if 'Risk Level' in str(worksheet.cell(row=1, column=cell.column).value):
                if cell.value == 'High':
                    cell.fill = PatternFill(start_color=risk_high, end_color=risk_high, fill_type="solid")
                elif cell.value == 'Medium':
                    cell.fill = PatternFill(start_color=risk_medium, end_color=risk_medium, fill_type="solid")
            
            # Spending variations highlighting
            if isinstance(cell.value, str):
                # Percentage variations
                if 'vs Avg' in str(worksheet.cell(row=1, column=cell.column).value):
                    try:
                        value = float(cell.value.replace('%', '').replace('+', ''))
                        if value > 20:
                            cell.fill = PatternFill(start_color=positive_var, end_color=positive_var, fill_type="solid")
                        elif value < -20:
                            cell.fill = PatternFill(start_color=negative_var, end_color=negative_var, fill_type="solid")
                    except:
                        pass
                
                # Monthly spending variations
                elif cell.value.startswith('$'):
                    try:
                        value = float(cell.value.replace('$', '').replace(',', ''))
                        if value > 0:
                            row_values = [
                                float(str(c.value).replace('$', '').replace(',', ''))
                                for c in row
                                if isinstance(c.value, str) and c.value.startswith('$')
                            ]
                            if row_values:
                                row_avg = np.mean(row_values)
                                if value > row_avg * 1.5:
                                    cell.fill = PatternFill(start_color=positive_var, end_color=positive_var, fill_type="solid")
                                elif value < row_avg * 0.5:
                                    cell.fill = PatternFill(start_color=negative_var, end_color=negative_var, fill_type="solid")
                    except:
                        pass
    
    # Adjust column widths
    for column in worksheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        adjusted_width = min(max_length + 2, 50) * 1.2
        worksheet.column_dimensions[column_letter].width = adjusted_width
    
    # Add filters and freeze panes
    worksheet.freeze_panes = 'A2'
    worksheet.auto_filter.ref = worksheet.dimensions

def create_service_category_analysis(cleaned_data):
    """Create analysis of service category percentages for each customer"""
    
    # Use existing service domains function
    service_categories = get_service_domains()

    analysis_data = []
    
    for account_name, df in cleaned_data.items():
        try:
            if account_name == 'Industry Average':
                continue
                
            # Get total spend from first data row
            total_spend = get_account_total_spend(df)
            print(f"Processing {account_name} with total spend: ${total_spend:,.2f}")
            
            # Initialize category spending
            category_spend = {category: 0 for category in service_categories.keys()}
            
            # Process each service row
            for idx, row in df.iterrows():
                service_name = str(row.iloc[0])
                if pd.isna(service_name) or 'View AWS service' in service_name:
                    continue
                    
                spend = clean_amount(row.iloc[1])
                
                # Use the same domain matching logic as in analyze_service_domains
                domain = get_service_domain(service_name, service_categories)
                if domain:
                    category_spend[domain] += spend
                else:
                    category_spend['Uncategorized'] += spend
            
            # Calculate percentages and create row data
            row_data = {
                'Customer Name': account_name,
                'Total Spend': f"${total_spend:,.2f}"
            }
            
            # Add percentage and spend for each category
            for category in service_categories.keys():
                spend = category_spend[category]
                percentage = (spend / total_spend * 100) if total_spend > 0 else 0
                row_data[f"{category} %"] = f"{percentage:.2f}%"
                row_data[f"{category} Spend"] = f"${spend:,.2f}"
            
            # Add top categories
            top_categories = sorted(
                [(cat, float(row_data[f"{cat} %"].replace('%', ''))) 
                 for cat in service_categories.keys()],
                key=lambda x: x[1],
                reverse=True
            )[:3]
            
            row_data['Top Categories'] = '; '.join(
                f"{cat} ({pct:.1f}%)" 
                for cat, pct in top_categories 
                if pct > 0
            )
            
            analysis_data.append(row_data)
            
        except Exception as e:
            print(f"Error processing {account_name}: {str(e)}")
            continue
    
    # Calculate industry average if we have data
    if analysis_data:
        avg_data = {
            'Customer Name': 'Industry Average',
            'Total Spend': f"${np.mean([float(d['Total Spend'].replace('$', '').replace(',', '')) for d in analysis_data]):,.2f}"
        }
        
        # Calculate averages for each category
        for category in service_categories.keys():
            spends = [float(d[f"{category} Spend"].replace('$', '').replace(',', '')) for d in analysis_data]
            avg_spend = np.mean(spends)
            
            total_avg_spend = float(avg_data['Total Spend'].replace('$', '').replace(',', ''))
            percentage = (avg_spend / total_avg_spend * 100) if total_avg_spend > 0 else 0
            
            avg_data[f"{category} %"] = f"{percentage:.2f}%"
            avg_data[f"{category} Spend"] = f"${avg_spend:,.2f}"
        
        # Add industry average top categories
        top_categories = sorted(
            [(cat, float(avg_data[f"{cat} %"].replace('%', ''))) 
             for cat in service_categories.keys()],
            key=lambda x: x[1],
            reverse=True
        )[:3]
        
        avg_data['Top Categories'] = '; '.join(
            f"{cat} ({pct:.1f}%)" 
            for cat, pct in top_categories 
            if pct > 0
        )
        
        analysis_data.append(avg_data)
    
    # Create DataFrame and sort by Total Spend
    df_analysis = pd.DataFrame(analysis_data)
    
    # Add category distribution score
    df_analysis['Category Distribution Score'] = df_analysis.apply(
        lambda row: calculate_category_distribution_score(row, service_categories.keys()), 
        axis=1
    )
    
    # Sort by total spend
    df_analysis['Total Spend Numeric'] = df_analysis['Total Spend'].apply(
        lambda x: float(x.replace('$', '').replace(',', ''))
    )
    df_analysis = df_analysis.sort_values('Total Spend Numeric', ascending=False)
    df_analysis = df_analysis.drop('Total Spend Numeric', axis=1)
    
    return df_analysis

def calculate_category_distribution_score(row, categories):
    """Calculate the distribution score for service categories"""
    try:
        percentages = []
        for category in categories:
            if f"{category} %" in row:
                percentage = float(str(row[f"{category} %"]).replace('%', ''))
                if percentage > 0:
                    percentages.append(percentage)
        
        if percentages:
            # Higher score means more even distribution
            return round(100 - np.std(percentages), 2)
        return 0.0
    except Exception as e:
        print(f"Error calculating distribution score: {str(e)}")
        return 0.0


def calculate_distribution_score(row, percentage_cols):
    """Calculate a score representing how well-distributed the spending is across categories"""
    try:
        percentages = [float(str(row[col]).replace('%', '')) for col in percentage_cols]
        non_zero = [p for p in percentages if p > 0]
        if not non_zero:
            return "0.00"
        
        # Calculate Gini coefficient (0 = perfect distribution, 1 = complete concentration)
        gini = 1 - (1 / len(non_zero)) * (2 * sum((i+1) * v for i, v in enumerate(sorted(non_zero))) / sum(non_zero) - (len(non_zero) + 1))
        
        # Convert to distribution score (100 = perfect distribution, 0 = complete concentration)
        score = (1 - gini) * 100
        return f"{score:.2f}"
    except:
        return "0.00"

def get_top_categories(row, percentage_cols):
    """Get the top 3 categories by percentage"""
    try:
        categories = [(col.replace(' %', ''), float(str(row[col]).replace('%', ''))) 
                     for col in percentage_cols]
        top_cats = sorted(categories, key=lambda x: x[1], reverse=True)[:3]
        return '; '.join(f"{cat} ({pct:.1f}%)" for cat, pct in top_cats if pct > 0)
    except:
        return "None"


def save_analysis_to_excel(output_file, df_summary, df_comparative, df_trends, df_monthly_trends, df_cumulative, cleaned_data):
    """Save all analysis results to Excel file with comprehensive formatting"""
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Create and save service category analysis
        df_service_categories = create_service_category_analysis(cleaned_data)
        df_service_categories.to_excel(writer, sheet_name='Service Categories', index=False)
        
        # Save other analyses
        df_summary.to_excel(writer, sheet_name='Domain Summary', index=False)
        df_comparative.to_excel(writer, sheet_name='Account Comparison', index=False)
        df_trends.to_excel(writer, sheet_name='Monthly Trends', index=True)
        df_monthly_trends.to_excel(writer, sheet_name='Detailed Analysis', index=False)
        df_cumulative.to_excel(writer, sheet_name='Cumulative Analysis', index=False)
        
        workbook = writer.book
        
        # Format Service Categories sheet
        service_cat_sheet = workbook['Service Categories']
        format_service_category_sheet(service_cat_sheet)
        
        # Format other sheets
        for sheet_name in workbook.sheetnames:
            if sheet_name != 'Service Categories':
                format_excel_with_highlights(workbook[sheet_name], sheet_name)

def format_service_category_sheet(worksheet):
    """Format the service category analysis sheet"""
    # Colors
    header_color = "1F4E78"
    alt_row = "F5F5F5"
    highlight_threshold = "90EE90"  # Light green for high percentages
    
    # Format headers
    for cell in worksheet[1]:
        cell.fill = PatternFill(start_color=header_color, end_color=header_color, fill_type="solid")
        cell.font = Font(color="FFFFFF", bold=True)
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    
    # Format data rows
    for row_idx, row in enumerate(worksheet.iter_rows(min_row=2), 2):
        fill_color = alt_row if row_idx % 2 == 0 else "FFFFFF"
        
        for cell_idx, cell in enumerate(row):
            cell.alignment = Alignment(horizontal='center', vertical='center')
            
            # Basic cell formatting
            if cell_idx > 1:  # Percentage columns
                try:
                    value = float(str(cell.value).replace('%', ''))
                    if value > 20:  # Highlight significant percentages
                        cell.fill = PatternFill(start_color=highlight_threshold, end_color=highlight_threshold, fill_type="solid")
                    else:
                        cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
                except:
                    cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
            else:
                cell.fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
                cell.font = Font(bold=True)
    
    # Adjust column widths
    for column in worksheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        adjusted_width = min(max_length + 2, 40) * 1.2
        worksheet.column_dimensions[column_letter].width = adjusted_width
    
    # Freeze panes and add filters
    worksheet.freeze_panes = 'A2'
    worksheet.auto_filter.ref = worksheet.dimensions

def save_analysis_to_excel(output_file, df_summary, df_comparative, df_trends, df_monthly_trends, df_cumulative, cleaned_data):
    """Save all analysis results to Excel file with comprehensive formatting"""
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Create service category analysis
            print("Creating Service Category Analysis...")
            df_service_categories = create_service_category_analysis(cleaned_data)
            
            # Save sheets, checking for None values
            sheets_to_save = {
                'Domain Summary': df_summary,
                'Account Comparison': df_comparative,
                'Monthly Trends': df_trends,
                'Detailed Analysis': df_monthly_trends,
                'Cumulative Analysis': df_cumulative
            }
            
            # Add service categories if available
            if df_service_categories is not None:
                sheets_to_save['Service Categories'] = df_service_categories
            
            # Save each sheet that has data
            sheets_saved = 0
            for sheet_name, df in sheets_to_save.items():
                if df is not None and not df.empty:
                    print(f"Saving {sheet_name} sheet...")
                    if sheet_name == 'Monthly Trends':
                        df.to_excel(writer, sheet_name=sheet_name, index=True)
                    else:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheets_saved += 1
            
            # If no sheets were saved, create a dummy sheet to prevent Excel errors
            if sheets_saved == 0:
                print("Warning: No data to save. Creating empty summary sheet.")
                pd.DataFrame({'No Data': ['No analysis results available']}).to_excel(
                    writer, sheet_name='Summary', index=False
                )
            
            # Format sheets
            workbook = writer.book
            for sheet_name in workbook.sheetnames:
                if sheet_name == 'Service Categories' and df_service_categories is not None:
                    format_service_category_sheet(workbook[sheet_name])
                else:
                    format_excel_with_highlights(workbook[sheet_name], sheet_name)
            
            print(f"Successfully saved {sheets_saved} sheets to {output_file}")
    
    except Exception as e:
        print(f"Error saving Excel file: {str(e)}")
        print("Attempting to save with minimal formatting...")
        
        # Fallback save with minimal formatting
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            if df_summary is not None and not df_summary.empty:
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
            else:
                pd.DataFrame({'Error': ['Error occurred during analysis']}).to_excel(
                    writer, sheet_name='Summary', index=False
                )


if __name__ == "__main__":
    try:
        excel_file_path = 'MyPodData.xlsx'
        
        if not os.path.exists(excel_file_path):
            raise FileNotFoundError(f"The file {excel_file_path} does not exist")
        
        print("Starting AWS spend analysis...")
        print("=" * 50)
        
        # Load and clean data
        print("\nStep 1: Loading and cleaning data...")
        cleaned_data = clean_excel_data(excel_file_path)
        
        if not cleaned_data:
            raise ValueError("No valid data found in Excel file")
        
        # Perform analyses
        print("\nStep 2: Performing comprehensive analysis...")
        
        # Domain Summary Analysis
        print("Processing Domain Summary...")
        df_summary, account_domain_spend = analyze_service_domains(cleaned_data)
        
        # Comparative Analysis
        print("Creating Comparative Analysis...")
        df_comparative, df_trends = create_comparative_analysis(cleaned_data, account_domain_spend)
        
        # Monthly Patterns Analysis
        print("Analyzing Monthly Patterns...")
        monthly_patterns = analyze_monthly_patterns(cleaned_data, account_domain_spend)
        df_monthly_trends = create_monthly_trends_report(monthly_patterns)
        
        # Cumulative Analysis
        print("Generating Cumulative Analysis...")
        df_cumulative = create_cumulative_report(cleaned_data, account_domain_spend)
        
        # Save to Excel
        output_file = 'AWS_Complete_Spend_Analysis.xlsx'
        print(f"\nStep 3: Saving analysis to {output_file}...")
        save_analysis_to_excel(
            output_file,
            df_summary,
            df_comparative,
            df_trends,
            df_monthly_trends,
            df_cumulative,
            cleaned_data
        )
        
        print("\nAnalysis Complete!")
        print("=" * 50)
        print("\nOutput Excel file contains:")
        print("1. Service Categories:")
        print("   - Detailed breakdown of service usage percentages")
        print("   - Industry average comparisons")
        print("   - Service concentration analysis")
        
        print("\n2. Domain Summary:")
        print("   - Overall service domain spending overview")
        print("   - Domain-wise total and average spend")
        print("   - Account distribution across domains")
        
        print("\n3. Account Comparison:")
        print("   - Account-level spending patterns")
        print("   - Domain concentration analysis")
        print("   - Month-over-month changes")
        
        print("\n4. Monthly Trends:")
        print("   - Monthly spending patterns")
        print("   - Seasonal variations")
        print("   - Growth trends")
        
        print("\n5. Detailed Analysis:")
        print("   - Comprehensive spending analysis")
        print("   - Anomaly detection")
        print("   - Risk assessment")
        
        print("\n6. Cumulative Analysis:")
        print("   - Cross-domain comparisons")
        print("   - Account spending distributions")
        print("   - Domain utilization patterns")
        
        # Print key findings
        print("\nKey Findings:")
        print("-" * 30)
        
        # Service Category Insights
        df_service_categories = create_service_category_analysis(cleaned_data)
        print("\nTop Service Categories:")
        for _, row in df_service_categories.iterrows():
            if row['Customer Name'] == 'Industry Average':
                for col in df_service_categories.columns:
                    if col.endswith('%'):
                        value = float(row[col].replace('%', ''))
                        if value > 10:  # Show categories with >10% usage
                            print(f"- {col.replace(' %', '')}: {row[col]}")
        
        # High risk patterns
        high_risk_patterns = df_monthly_trends[df_monthly_trends['Risk Level'] == 'High']
        if not high_risk_patterns.empty:
            print("\nHigh Risk Spending Patterns Detected:")
            for _, pattern in high_risk_patterns.iterrows():
                print(f"- {pattern['Account']} - {pattern['Service Domain']}")
                print(f"  * Pattern: {pattern['Spending Pattern']}")
                print(f"  * Changes: {pattern['Notable Changes']}")
        
        # Top spending domains
        top_domains = df_summary.head(3)
        print("\nTop 3 Spending Domains:")
        for _, domain in top_domains.iterrows():
            print(f"- {domain['Service Domain']}: {domain['Total Spend']}")
        
        # Significant variations
        significant_variations = df_monthly_trends[
            df_monthly_trends['Volatility'].str.rstrip('%').astype(float) > 50
        ]
        if not significant_variations.empty:
            print("\nSignificant Spending Variations:")
            for _, var in significant_variations.iterrows():
                print(f"- {var['Account']} - {var['Service Domain']}")
                print(f"  * Volatility: {var['Volatility']}")
        
        print("\nRecommended Actions:")
        print("1. Review high-risk spending patterns")
        print("2. Investigate significant variations")
        print("3. Optimize top spending domains")
        print("4. Monitor monthly trends for cost optimization")
        print("5. Review service category distribution")
        
        print("\nAnalysis file saved successfully!")
        print(f"Location: {os.path.abspath(output_file)}")
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        print("\nStack trace:")
        import traceback
        traceback.print_exc()
        raise
